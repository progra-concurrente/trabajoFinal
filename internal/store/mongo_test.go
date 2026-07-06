package store

import (
	"context"
	"os"
	"strings"
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

func TestMongoUsersAndAuthentication(t *testing.T) {
	uri := os.Getenv("TEST_MONGO_URI")
	if uri == "" {
		t.Skip("TEST_MONGO_URI is not configured")
	}
	ctx := context.Background()
	db, err := Open(ctx, uri, "powersight_user_test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	username := "Alumno_" + strings.ReplaceAll(t.Name(), "/", "_")
	user, err := db.CreateUser(ctx, username, "powersight123", "user")
	if err != nil {
		t.Fatal(err)
	}
	if user.PasswordHash == "powersight123" || user.Role != "user" {
		t.Fatalf("unexpected user document: %+v", user)
	}
	authenticated, err := db.AuthenticateUser(ctx, username, "powersight123")
	if err != nil {
		t.Fatal(err)
	}
	if authenticated.Username != strings.ToLower(username) {
		t.Fatalf("unexpected username %q", authenticated.Username)
	}
	if _, err := db.AuthenticateUser(ctx, username, "wrong-password"); err == nil {
		t.Fatal("expected invalid credentials")
	}
}
