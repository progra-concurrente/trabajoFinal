package ml

import (
	"math"
	"testing"
)

func validFeatures(active float64) ForecastFeatures {
	return ForecastFeatures{
		CurrentActivePower: active, RecentAverageActivePower: active, RecentMaximumActivePower: active,
		CurrentVoltage: 240, Hour: 20, DayOfWeek: 1, Month: 6,
		HistoricalHourHighRate: .5, HistoricalDayHourHighRate: .6,
	}
}

func TestPredictFutureRisk(t *testing.T) {
	model := Model{Weights: make([]float64, len(FeatureNames)+1), DecisionThreshold: .5}
	prediction, err := Predict(model, validFeatures(1))
	if err != nil {
		t.Fatal(err)
	}
	if prediction.Probability != .5 || prediction.Class != 1 {
		t.Fatalf("unexpected prediction: %+v", prediction)
	}
}

func TestConcurrentGradientMatchesSequential(t *testing.T) {
	records := []Record{
		{ForecastFeatures: validFeatures(.5), FutureHighConsumption: 0},
		{ForecastFeatures: validFeatures(2), FutureHighConsumption: 1},
		{ForecastFeatures: validFeatures(.8), FutureHighConsumption: 0},
		{ForecastFeatures: validFeatures(3), FutureHighConsumption: 1},
	}
	weights := make([]float64, len(FeatureNames)+1)
	sequential := ComputeGradient(records, weights)
	concurrent := ComputeGradientConcurrent(records, weights, 3)
	for index := range sequential.Values {
		if math.Abs(sequential.Values[index]-concurrent.Values[index]) > 1e-12 {
			t.Fatalf("gradient %d differs", index)
		}
	}
	if math.Abs(sequential.Loss-concurrent.Loss) > 1e-12 || sequential.Count != concurrent.Count {
		t.Fatalf("results differ")
	}
}

func TestAggregateDistributedGradients(t *testing.T) {
	records := []Record{{ForecastFeatures: validFeatures(.5)}, {ForecastFeatures: validFeatures(2), FutureHighConsumption: 1}}
	weights := make([]float64, len(FeatureNames)+1)
	whole, _ := AggregateGradients([]Gradient{ComputeGradient(records, weights)}, len(weights))
	distributed, _ := AggregateGradients([]Gradient{ComputeGradient(records[:1], weights), ComputeGradient(records[1:], weights)}, len(weights))
	for index := range whole.Values {
		if math.Abs(whole.Values[index]-distributed.Values[index]) > 1e-12 {
			t.Fatal("distributed gradient differs")
		}
	}
}
