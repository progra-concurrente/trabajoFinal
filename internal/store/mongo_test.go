package store

import (
	"context"
	"os"
	"testing"

	"powersight/pkg/ml"
)

func TestMongoIndexesAndInitialModel(t *testing.T) {
	uri := os.Getenv("TEST_MONGO_URI")
	if uri == "" {
		t.Skip("TEST_MONGO_URI is not configured")
	}
	ctx := context.Background()
	db, err := Open(ctx, uri, "powersight_test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	model, err := db.EnsureInitialModel(ctx, ml.Model{
		Version: "integration-test", ModelType: "logistic_regression",
		Target: "FutureSustainedHighConsumption30m", Features: ml.FeatureNames,
		Weights:      make([]float64, len(ml.FeatureNames)+1),
		LearningRate: .25, Epochs: 1, Threshold: 1.528, DecisionThreshold: .5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if model.ID == "" {
		t.Fatal("expected model id")
	}
}
