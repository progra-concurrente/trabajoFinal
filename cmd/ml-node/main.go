package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"powersight/pkg/ml"
	"powersight/pkg/protocol"
)

func main() {
	nodeID := env("NODE_ID", "ml-node-1")
	coordinator := env("COORDINATOR_TCP_ADDR", "localhost:9000")
	datasetPath := env("TRAINING_DATA_PATH", "data/processed/forecast_training.csv")
	capacity := envInt("NODE_CAPACITY", 4)
	retry := envDuration("RECONNECT_INTERVAL", 2*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for ctx.Err() == nil {
		if err := run(ctx, nodeID, coordinator, datasetPath, capacity); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker disconnected: %v", err)
		}
		select {
		case <-time.After(retry):
		case <-ctx.Done():
		}
	}
}

func run(ctx context.Context, nodeID, address, datasetPath string, capacity int) error {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)
	register, _ := protocol.NewMessage(fmt.Sprintf("register-%d", time.Now().UnixNano()), protocol.Register, protocol.Registration{
		NodeID: nodeID, Capacity: capacity, ModelVersion: "initial",
	})
	register.NodeID = nodeID
	if err := protocol.Encode(writer, register); err != nil {
		return err
	}
	if _, err := protocol.Decode(reader); err != nil {
		return err
	}
	log.Printf("%s connected to coordinator %s", nodeID, address)
	runtime := &workerRuntime{datasetPath: datasetPath, capacity: capacity}
	var writeMu sync.Mutex
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	for {
		message, err := protocol.Decode(reader)
		if err != nil {
			return err
		}
		go func(message protocol.Message) {
			response := runtime.handle(message)
			response.NodeID = nodeID
			writeMu.Lock()
			defer writeMu.Unlock()
			if err := protocol.Encode(writer, response); err != nil {
				_ = conn.Close()
			}
		}(message)
	}
}

type workerRuntime struct {
	datasetPath string
	capacity    int
	activeJobs  int64
	loadMu      sync.Mutex
	records     []ml.Record
}

func (w *workerRuntime) loadRecords() ([]ml.Record, error) {
	w.loadMu.Lock()
	defer w.loadMu.Unlock()
	if w.records != nil {
		return w.records, nil
	}
	records, err := ml.LoadCSVRange(w.datasetPath, 0, -1)
	if err != nil {
		return nil, err
	}
	w.records = records
	return w.records, nil
}

func (w *workerRuntime) handle(message protocol.Message) protocol.Message {
	errorMessage := func(err error) protocol.Message {
		return protocol.Message{ID: message.ID, Type: protocol.Error, Error: err.Error()}
	}
	switch message.Type {
	case protocol.Heartbeat:
		records := 0
		if w.records != nil {
			records = len(w.records)
		}
		activeJobs := int(atomic.LoadInt64(&w.activeJobs))
		cpu := 0.0
		if w.capacity > 0 {
			cpu = float64(activeJobs) / float64(w.capacity) * 100
			if cpu > 100 {
				cpu = 100
			}
		}
		response, _ := protocol.NewMessage(message.ID, protocol.Heartbeat, protocol.HeartbeatStatus{
			ReceivedAt: time.Now().UnixMilli(), ActiveJobs: activeJobs, Capacity: w.capacity,
			CPUUsagePercent: cpu, LoadedRecordRows: records,
		})
		return response
	case protocol.Predict:
		atomic.AddInt64(&w.activeJobs, 1)
		defer atomic.AddInt64(&w.activeJobs, -1)
		var payload struct {
			Model    ml.Model            `json:"model"`
			Features ml.ForecastFeatures `json:"features"`
		}
		if err := protocol.UnmarshalPayload(message, &payload); err != nil {
			return errorMessage(err)
		}
		prediction, err := ml.Predict(payload.Model, payload.Features)
		if err != nil {
			return errorMessage(err)
		}
		response, _ := protocol.NewMessage(message.ID, protocol.Predict, prediction)
		return response
	case protocol.TrainInit:
		atomic.AddInt64(&w.activeJobs, 1)
		defer atomic.AddInt64(&w.activeJobs, -1)
		records, err := w.loadRecords()
		if err != nil {
			return errorMessage(err)
		}
		response, _ := protocol.NewMessage(message.ID, protocol.TrainInit, protocol.TrainInitResult{Rows: len(records)})
		return response
	case protocol.TrainEpoch:
		atomic.AddInt64(&w.activeJobs, 1)
		defer atomic.AddInt64(&w.activeJobs, -1)
		var payload protocol.TrainEpochPayload
		if err := protocol.UnmarshalPayload(message, &payload); err != nil {
			return errorMessage(err)
		}
		records, err := w.loadRecords()
		if err != nil {
			return errorMessage(err)
		}
		if payload.Start < 0 || payload.End > len(records) || payload.Start >= payload.End {
			return errorMessage(fmt.Errorf("invalid training range %d:%d for %d rows", payload.Start, payload.End, len(records)))
		}
		gradient := ml.ComputeGradientConcurrent(records[payload.Start:payload.End], payload.Weights, w.capacity)
		response, _ := protocol.NewMessage(message.ID, protocol.GradientResult, gradient)
		return response
	default:
		return errorMessage(fmt.Errorf("unsupported message type %q", message.Type))
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}
