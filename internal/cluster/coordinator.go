package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"powersight/pkg/ml"
	"powersight/pkg/protocol"
)

type Event struct {
	NodeID  string
	Type    string
	Details string
}

type EventSink func(context.Context, Event)

type NodeInfo struct {
	ID             string    `json:"id"`
	RemoteAddress  string    `json:"remote_address"`
	Capacity       int       `json:"capacity"`
	ModelVersion   string    `json:"model_version"`
	Healthy        bool      `json:"healthy"`
	ActiveJobs     int       `json:"active_jobs"`
	CPUUsage       float64   `json:"cpu_usage_percent"`
	LoadedRows     int       `json:"loaded_record_rows"`
	JobsCompleted  uint64    `json:"jobs_completed"`
	Errors         uint64    `json:"errors"`
	AverageLatency float64   `json:"average_latency_ms"`
	LastSeen       time.Time `json:"last_seen"`
}

type PredictionResult struct {
	ml.Prediction
	NodeID    string        `json:"node_id"`
	Latency   time.Duration `json:"-"`
	LatencyMS float64       `json:"latency_ms"`
}

type TrainOptions struct {
	Epochs       int
	LearningRate float64
}

type EpochMetric struct {
	Epoch int     `json:"epoch"`
	Loss  float64 `json:"loss"`
	Rows  int     `json:"rows"`
}

type Coordinator struct {
	address           string
	requestTimeout    time.Duration
	heartbeatInterval time.Duration
	missedHeartbeats  int
	eventSink         EventSink

	mu       sync.RWMutex
	nodes    map[string]*nodeClient
	rr       uint64
	listener net.Listener
	closed   chan struct{}
}

type nodeClient struct {
	info       NodeInfo
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	writeMu    sync.Mutex
	pendingMu  sync.Mutex
	pending    map[string]chan protocol.Message
	failures   int
	totalNanos int64
	closed     chan struct{}
	closeOnce  sync.Once
}

var requestSequence uint64

func New(address string, requestTimeout, heartbeatInterval time.Duration, missedHeartbeats int, sink EventSink) *Coordinator {
	if requestTimeout <= 0 {
		requestTimeout = 5 * time.Second
	}
	if heartbeatInterval <= 0 {
		heartbeatInterval = 3 * time.Second
	}
	if missedHeartbeats <= 0 {
		missedHeartbeats = 3
	}
	return &Coordinator{
		address: address, requestTimeout: requestTimeout, heartbeatInterval: heartbeatInterval,
		missedHeartbeats: missedHeartbeats, eventSink: sink, nodes: make(map[string]*nodeClient),
		closed: make(chan struct{}),
	}
}

func (c *Coordinator) Start() error {
	listener, err := net.Listen("tcp", c.address)
	if err != nil {
		return err
	}
	c.listener = listener
	go c.acceptLoop()
	go c.heartbeatLoop()
	return nil
}

func (c *Coordinator) Addr() string {
	if c.listener == nil {
		return c.address
	}
	return c.listener.Addr().String()
}

func (c *Coordinator) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	if c.listener != nil {
		_ = c.listener.Close()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.nodes {
		node.close()
	}
	return nil
}

func (c *Coordinator) acceptLoop() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			select {
			case <-c.closed:
				return
			default:
				continue
			}
		}
		go c.registerConnection(conn)
	}
}

func (c *Coordinator) registerConnection(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(c.requestTimeout))
	reader := bufio.NewReader(conn)
	message, err := protocol.Decode(reader)
	if err != nil || message.Type != protocol.Register {
		_ = conn.Close()
		return
	}
	var registration protocol.Registration
	if protocol.UnmarshalPayload(message, &registration) != nil || registration.NodeID == "" {
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	node := &nodeClient{
		info: NodeInfo{
			ID: registration.NodeID, RemoteAddress: conn.RemoteAddr().String(),
			Capacity: registration.Capacity, ModelVersion: registration.ModelVersion,
			Healthy: true, LastSeen: time.Now(),
		},
		conn: conn, reader: reader, writer: bufio.NewWriter(conn),
		pending: make(map[string]chan protocol.Message), closed: make(chan struct{}),
	}
	c.mu.Lock()
	if old := c.nodes[registration.NodeID]; old != nil {
		old.close()
	}
	c.nodes[registration.NodeID] = node
	c.mu.Unlock()
	response, _ := protocol.NewMessage(message.ID, protocol.Register, map[string]bool{"accepted": true})
	_ = node.send(response)
	c.emit(Event{NodeID: registration.NodeID, Type: "connected", Details: conn.RemoteAddr().String()})
	go c.readLoop(node)
}

func (c *Coordinator) readLoop(node *nodeClient) {
	defer c.markDisconnected(node, "connection closed")
	for {
		message, err := protocol.Decode(node.reader)
		if err != nil {
			return
		}
		node.pendingMu.Lock()
		response := node.pending[message.ID]
		if response != nil {
			delete(node.pending, message.ID)
		}
		node.pendingMu.Unlock()
		if response != nil {
			response <- message
			close(response)
		}
	}
}

func (c *Coordinator) heartbeatLoop() {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, node := range c.snapshotNodes(false) {
				ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
				response, err := c.call(ctx, node, protocol.Heartbeat, map[string]int64{"sent_at": time.Now().UnixMilli()})
				cancel()
				c.mu.Lock()
				if current := c.nodes[node.info.ID]; current == node {
					if err != nil {
						node.failures++
						atomic.AddUint64(&node.info.Errors, 1)
						if node.failures >= c.missedHeartbeats {
							node.info.Healthy = false
						}
					} else {
						node.failures = 0
						node.info.Healthy = true
						node.info.LastSeen = time.Now()
						var status protocol.HeartbeatStatus
						if protocol.UnmarshalPayload(response, &status) == nil {
							node.info.ActiveJobs = status.ActiveJobs
							node.info.CPUUsage = status.CPUUsagePercent
							node.info.LoadedRows = status.LoadedRecordRows
							if status.Capacity > 0 {
								node.info.Capacity = status.Capacity
							}
						}
					}
				}
				c.mu.Unlock()
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Coordinator) Predict(ctx context.Context, model ml.Model, features ml.ForecastFeatures) (PredictionResult, error) {
	nodes := c.healthyNodes()
	if len(nodes) == 0 {
		return PredictionResult{}, errors.New("no healthy ML nodes available")
	}
	startIndex := int(atomic.AddUint64(&c.rr, 1)-1) % len(nodes)
	var lastErr error
	for attempt := 0; attempt < len(nodes); attempt++ {
		node := nodes[(startIndex+attempt)%len(nodes)]
		start := time.Now()
		response, err := c.call(ctx, node, protocol.Predict, struct {
			Model    ml.Model            `json:"model"`
			Features ml.ForecastFeatures `json:"features"`
		}{model, features})
		latency := time.Since(start)
		if err != nil {
			lastErr = err
			atomic.AddUint64(&node.info.Errors, 1)
			c.emit(Event{NodeID: node.info.ID, Type: "prediction_reassigned", Details: err.Error()})
			continue
		}
		var prediction ml.Prediction
		if err := protocol.UnmarshalPayload(response, &prediction); err != nil {
			lastErr = err
			continue
		}
		atomic.AddUint64(&node.info.JobsCompleted, 1)
		atomic.AddInt64(&node.totalNanos, latency.Nanoseconds())
		return PredictionResult{
			Prediction: prediction, NodeID: node.info.ID, Latency: latency,
			LatencyMS: float64(latency) / float64(time.Millisecond),
		}, nil
	}
	return PredictionResult{}, fmt.Errorf("prediction failed on all nodes: %w", lastErr)
}

func (c *Coordinator) Train(ctx context.Context, initial ml.Model, options TrainOptions, onEpoch func(EpochMetric)) (ml.Model, error) {
	if options.Epochs <= 0 {
		options.Epochs = 80
	}
	if options.LearningRate <= 0 {
		options.LearningRate = 0.25
	}
	nodes := c.healthyNodes()
	if len(nodes) == 0 {
		return ml.Model{}, errors.New("no healthy ML nodes available")
	}
	var initResult protocol.TrainInitResult
	var initErr error
	for _, node := range nodes {
		initResponse, err := c.call(ctx, node, protocol.TrainInit, protocol.TrainInitPayload{})
		if err != nil {
			initErr = err
			continue
		}
		if err := protocol.UnmarshalPayload(initResponse, &initResult); err != nil {
			initErr = err
			continue
		}
		initErr = nil
		break
	}
	if initErr != nil {
		return ml.Model{}, fmt.Errorf("could not initialize training dataset: %w", initErr)
	}
	if initResult.Rows == 0 {
		return ml.Model{}, errors.New("training dataset is empty")
	}
	weights := append([]float64(nil), initial.Weights...)
	if len(weights) != len(ml.FeatureNames)+1 {
		weights = make([]float64, len(ml.FeatureNames)+1)
	}
	for epoch := 1; epoch <= options.Epochs; epoch++ {
		active := c.healthyNodes()
		if len(active) == 0 {
			return ml.Model{}, errors.New("all ML nodes became unavailable")
		}
		shards := splitRanges(initResult.Rows, len(active))
		gradients := make([]ml.Gradient, len(shards))
		errs := make(chan error, len(shards))
		var wg sync.WaitGroup
		for index, shard := range shards {
			wg.Add(1)
			go func(index int, shard indexRange) {
				defer wg.Done()
				gradient, err := c.computeShard(ctx, active, index, shard, weights)
				if err == nil {
					gradients[index] = gradient
				}
				errs <- err
			}(index, shard)
		}
		wg.Wait()
		close(errs)
		for shardErr := range errs {
			if shardErr != nil {
				return ml.Model{}, shardErr
			}
		}
		aggregate, err := ml.AggregateGradients(gradients, len(weights))
		if err != nil {
			return ml.Model{}, err
		}
		weights = ml.ApplyGradient(weights, aggregate, options.LearningRate)
		if onEpoch != nil {
			onEpoch(EpochMetric{Epoch: epoch, Loss: aggregate.Loss, Rows: aggregate.Count})
		}
	}
	result := initial
	result.Weights = weights
	result.Epochs = options.Epochs
	result.LearningRate = options.LearningRate
	result.Version = fmt.Sprintf("distributed-%d", time.Now().UnixNano())
	c.mu.Lock()
	for _, node := range c.nodes {
		node.info.ModelVersion = result.Version
	}
	c.mu.Unlock()
	return result, nil
}

func (c *Coordinator) computeShard(ctx context.Context, nodes []*nodeClient, preferred int, shard indexRange, weights []float64) (ml.Gradient, error) {
	var lastErr error
	for attempt := 0; attempt < len(nodes); attempt++ {
		node := nodes[(preferred+attempt)%len(nodes)]
		response, err := c.call(ctx, node, protocol.TrainEpoch, protocol.TrainEpochPayload{
			Start: shard.Start, End: shard.End, Weights: weights,
		})
		if err != nil {
			lastErr = err
			c.emit(Event{NodeID: node.info.ID, Type: "training_shard_reassigned", Details: err.Error()})
			continue
		}
		var gradient ml.Gradient
		if err := protocol.UnmarshalPayload(response, &gradient); err != nil {
			lastErr = err
			continue
		}
		atomic.AddUint64(&node.info.JobsCompleted, 1)
		return gradient, nil
	}
	return ml.Gradient{}, fmt.Errorf("training shard %d-%d failed: %w", shard.Start, shard.End, lastErr)
}

func (c *Coordinator) call(ctx context.Context, node *nodeClient, messageType string, payload any) (protocol.Message, error) {
	id := fmt.Sprintf("req-%d", atomic.AddUint64(&requestSequence, 1))
	message, err := protocol.NewMessage(id, messageType, payload)
	if err != nil {
		return protocol.Message{}, err
	}
	response := make(chan protocol.Message, 1)
	node.pendingMu.Lock()
	node.pending[id] = response
	node.pendingMu.Unlock()
	if err := node.send(message); err != nil {
		node.removePending(id)
		return protocol.Message{}, err
	}
	select {
	case result := <-response:
		if result.Type == protocol.Error || result.Error != "" {
			return protocol.Message{}, errors.New(result.Error)
		}
		c.mu.Lock()
		if current := c.nodes[node.info.ID]; current == node {
			node.info.LastSeen = time.Now()
		}
		c.mu.Unlock()
		return result, nil
	case <-ctx.Done():
		node.removePending(id)
		return protocol.Message{}, ctx.Err()
	case <-time.After(c.requestTimeout):
		node.removePending(id)
		return protocol.Message{}, errors.New("ML node request timed out")
	case <-node.closed:
		node.removePending(id)
		return protocol.Message{}, errors.New("ML node disconnected")
	}
}

func (n *nodeClient) send(message protocol.Message) error {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	return protocol.Encode(n.writer, message)
}

func (n *nodeClient) removePending(id string) {
	n.pendingMu.Lock()
	delete(n.pending, id)
	n.pendingMu.Unlock()
}

func (n *nodeClient) close() {
	n.closeOnce.Do(func() {
		close(n.closed)
		_ = n.conn.Close()
	})
}

func (c *Coordinator) markDisconnected(node *nodeClient, detail string) {
	node.close()
	c.mu.Lock()
	if current := c.nodes[node.info.ID]; current == node {
		node.info.Healthy = false
	}
	c.mu.Unlock()
	c.emit(Event{NodeID: node.info.ID, Type: "disconnected", Details: detail})
}

func (c *Coordinator) Nodes() []NodeInfo {
	c.mu.RLock()
	result := make([]NodeInfo, 0, len(c.nodes))
	for _, node := range c.nodes {
		info := node.info
		jobs := atomic.LoadUint64(&node.info.JobsCompleted)
		info.JobsCompleted = jobs
		info.Errors = atomic.LoadUint64(&node.info.Errors)
		if jobs > 0 {
			info.AverageLatency = float64(atomic.LoadInt64(&node.totalNanos)) / float64(time.Millisecond) / float64(jobs)
		}
		result = append(result, info)
	}
	c.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (c *Coordinator) HealthyCount() int { return len(c.healthyNodes()) }

func (c *Coordinator) healthyNodes() []*nodeClient { return c.snapshotNodes(true) }

func (c *Coordinator) snapshotNodes(healthyOnly bool) []*nodeClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*nodeClient, 0, len(c.nodes))
	for _, node := range c.nodes {
		if !healthyOnly || node.info.Healthy {
			result = append(result, node)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].info.ID < result[j].info.ID })
	return result
}

func (c *Coordinator) emit(event Event) {
	if c.eventSink != nil {
		go c.eventSink(context.Background(), event)
	}
}

type indexRange struct{ Start, End int }

func splitRanges(total, count int) []indexRange {
	if count > total {
		count = total
	}
	result := make([]indexRange, 0, count)
	base, remainder, start := total/count, total%count, 0
	for i := 0; i < count; i++ {
		end := start + base
		if i < remainder {
			end++
		}
		result = append(result, indexRange{start, end})
		start = end
	}
	return result
}
