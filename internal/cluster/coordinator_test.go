package cluster

import (
	"bufio"
	"context"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"powersight/pkg/ml"
	"powersight/pkg/protocol"
)

func TestCoordinatorDistributesPredictionsAndSurvivesNodeFailure(t *testing.T) {
	coordinator := New("127.0.0.1:0", time.Second, 50*time.Millisecond, 1, nil)
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	stop1 := startFakeNode(t, coordinator.Addr(), "node-1")
	stop2 := startFakeNode(t, coordinator.Addr(), "node-2")
	defer stop1()
	defer stop2()
	waitForNodes(t, coordinator, 2)

	model := ml.Model{Weights: make([]float64, len(ml.FeatureNames)+1), DecisionThreshold: 0.5}
	features := testFeatures(1)
	used := map[string]bool{}
	for i := 0; i < 4; i++ {
		result, err := coordinator.Predict(context.Background(), model, features)
		if err != nil {
			t.Fatal(err)
		}
		used[result.NodeID] = true
	}
	if len(used) != 2 {
		t.Fatalf("round-robin did not use both nodes: %+v", used)
	}

	stop1()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if coordinator.HealthyCount() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	result, err := coordinator.Predict(context.Background(), model, features)
	if err != nil {
		t.Fatal(err)
	}
	if result.NodeID != "node-2" {
		t.Fatalf("expected surviving node, got %s", result.NodeID)
	}
}

func TestDistributedTrainingMatchesLocalGradientUpdates(t *testing.T) {
	records := []ml.Record{
		{ForecastFeatures: testFeatures(.5), FutureHighConsumption: 0},
		{ForecastFeatures: testFeatures(2), FutureHighConsumption: 1},
		{ForecastFeatures: testFeatures(.8), FutureHighConsumption: 0},
		{ForecastFeatures: testFeatures(3), FutureHighConsumption: 1},
	}
	coordinator := New("127.0.0.1:0", time.Second, time.Second, 2, nil)
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	stop1 := startTrainingNode(t, coordinator.Addr(), "node-1", records)
	stop2 := startTrainingNode(t, coordinator.Addr(), "node-2", records)
	defer stop1()
	defer stop2()
	waitForNodes(t, coordinator, 2)

	initial := ml.Model{
		Weights: make([]float64, len(ml.FeatureNames)+1), Features: ml.FeatureNames,
		ModelType: "logistic_regression", Target: "FutureSustainedHighConsumption30m",
		DecisionThreshold: 0.5, Threshold: 1.528,
	}
	distributed, err := coordinator.Train(context.Background(), initial, TrainOptions{
		Epochs: 3, LearningRate: 0.25,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	localWeights := append([]float64(nil), initial.Weights...)
	for epoch := 0; epoch < 3; epoch++ {
		gradient, err := ml.AggregateGradients([]ml.Gradient{
			ml.ComputeGradient(records, localWeights),
		}, len(localWeights))
		if err != nil {
			t.Fatal(err)
		}
		localWeights = ml.ApplyGradient(localWeights, gradient, 0.25)
	}
	for index := range localWeights {
		if math.Abs(localWeights[index]-distributed.Weights[index]) > 1e-12 {
			t.Fatalf("weight %d differs: local=%v distributed=%v", index, localWeights[index], distributed.Weights[index])
		}
	}
}

func testFeatures(active float64) ml.ForecastFeatures {
	return ml.ForecastFeatures{CurrentActivePower: active, RecentAverageActivePower: active,
		RecentMaximumActivePower: active, CurrentVoltage: 240, Hour: 20, DayOfWeek: 1, Month: 6,
		HistoricalHourHighRate: .5, HistoricalDayHourHighRate: .6}
}

func startFakeNode(t *testing.T, address, id string) func() {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = conn.Close() }) }
	go func() {
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		register, _ := protocol.NewMessage("register-"+id, protocol.Register, protocol.Registration{
			NodeID: id, Capacity: 1, ModelVersion: "test",
		})
		_ = protocol.Encode(writer, register)
		if _, err := protocol.Decode(reader); err != nil {
			return
		}
		for {
			message, err := protocol.Decode(reader)
			if err != nil {
				return
			}
			var response protocol.Message
			switch message.Type {
			case protocol.Heartbeat:
				response, _ = protocol.NewMessage(message.ID, protocol.Heartbeat, map[string]bool{"ok": true})
			case protocol.Predict:
				response, _ = protocol.NewMessage(message.ID, protocol.Predict, ml.Prediction{Probability: 0.75, Class: 1})
			default:
				response = protocol.Message{ID: message.ID, Type: protocol.Error, Error: "unsupported"}
			}
			if protocol.Encode(writer, response) != nil {
				return
			}
		}
	}()
	return stop
}

func startTrainingNode(t *testing.T, address, id string, records []ml.Record) func() {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = conn.Close() }) }
	go func() {
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		register, _ := protocol.NewMessage("register-"+id, protocol.Register, protocol.Registration{
			NodeID: id, Capacity: 1, ModelVersion: "test",
		})
		_ = protocol.Encode(writer, register)
		if _, err := protocol.Decode(reader); err != nil {
			return
		}
		for {
			message, err := protocol.Decode(reader)
			if err != nil {
				return
			}
			var response protocol.Message
			switch message.Type {
			case protocol.Heartbeat:
				response, _ = protocol.NewMessage(message.ID, protocol.Heartbeat, map[string]bool{"ok": true})
			case protocol.TrainInit:
				response, _ = protocol.NewMessage(message.ID, protocol.TrainInit, protocol.TrainInitResult{Rows: len(records)})
			case protocol.TrainEpoch:
				var payload protocol.TrainEpochPayload
				if protocol.UnmarshalPayload(message, &payload) != nil {
					return
				}
				gradient := ml.ComputeGradient(records[payload.Start:payload.End], payload.Weights)
				response, _ = protocol.NewMessage(message.ID, protocol.GradientResult, gradient)
			default:
				response = protocol.Message{ID: message.ID, Type: protocol.Error, Error: "unsupported"}
			}
			if protocol.Encode(writer, response) != nil {
				return
			}
		}
	}()
	return stop
}

func waitForNodes(t *testing.T, coordinator *Coordinator, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if coordinator.HealthyCount() == count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d nodes, got %d", count, coordinator.HealthyCount())
}
