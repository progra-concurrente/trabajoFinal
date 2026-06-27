package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"powersight/internal/cluster"
	"powersight/pkg/ml"
)

var ErrNotFound = errors.New("record not found")

type Store struct {
	client *mongo.Client
	db     *mongo.Database
}

type Recommendation struct {
	Title   string   `json:"title" bson:"title"`
	Message string   `json:"message" bson:"message"`
	Actions []string `json:"actions" bson:"actions"`
}

type TimeContext struct {
	TimeBand        string  `json:"time_band" bson:"time_band"`
	DayName         string  `json:"day_name" bson:"day_name"`
	MonthName       string  `json:"month_name" bson:"month_name"`
	Timezone        string  `json:"timezone" bson:"timezone"`
	IsPeak          bool    `json:"is_peak" bson:"is_peak"`
	HourHighRate    float64 `json:"hour_high_rate" bson:"hour_high_rate"`
	DayHourHighRate float64 `json:"day_hour_high_rate" bson:"day_hour_high_rate"`
}

type CurrentStatus struct {
	ActivePowerKW     float64 `json:"active_power_kw" bson:"active_power_kw"`
	ThresholdKW       float64 `json:"threshold_kw" bson:"threshold_kw"`
	Level             string  `json:"level" bson:"level"`
	CurrentlyHigh     bool    `json:"currently_high" bson:"currently_high"`
	UnmeteredEnergyWh float64 `json:"unmetered_energy_wh" bson:"unmetered_energy_wh"`
}

type ForecastRecord struct {
	ID                  string              `json:"id" bson:"_id"`
	UserID              string              `json:"user_id" bson:"user_id"`
	ObservedAt          time.Time           `json:"observed_at" bson:"observed_at"`
	ObservedAtLocal     string              `json:"observed_at_local" bson:"observed_at_local"`
	CurrentStatus       CurrentStatus       `json:"current_status" bson:"current_status"`
	Features            ml.ForecastFeatures `json:"features" bson:"features"`
	HorizonMinutes      int                 `json:"horizon_minutes" bson:"horizon_minutes"`
	ExpectedWindowStart time.Time           `json:"expected_window_start" bson:"expected_window_start"`
	ExpectedWindowEnd   time.Time           `json:"expected_window_end" bson:"expected_window_end"`
	Probability         float64             `json:"probability" bson:"probability"`
	Class               int                 `json:"class" bson:"class"`
	RiskLevel           string              `json:"risk_level" bson:"risk_level"`
	Context             TimeContext         `json:"context" bson:"context"`
	Recommendation      Recommendation      `json:"recommendation" bson:"recommendation"`
	NodeID              string              `json:"node_id" bson:"node_id"`
	ModelID             string              `json:"model_id" bson:"model_id"`
	ModelVersion        string              `json:"model_version" bson:"model_version"`
	ClusterLatencyMS    float64             `json:"cluster_latency_ms" bson:"cluster_latency_ms"`
	ProcessingTimeMS    float64             `json:"processing_time_ms" bson:"processing_time_ms"`
	PerformanceTargetMS float64             `json:"performance_target_ms" bson:"performance_target_ms"`
	TargetMet           bool                `json:"target_met" bson:"target_met"`
	Cached              bool                `json:"cached" bson:"cached"`
	CreatedAt           time.Time           `json:"created_at" bson:"created_at"`
}

type TrainingRun struct {
	ID           string           `json:"id" bson:"_id"`
	Status       string           `json:"status" bson:"status"`
	Epochs       int              `json:"epochs" bson:"epochs"`
	CurrentEpoch int              `json:"current_epoch" bson:"current_epoch"`
	LearningRate float64          `json:"learning_rate" bson:"learning_rate"`
	Workers      int              `json:"workers" bson:"workers"`
	ModelID      string           `json:"model_id,omitempty" bson:"model_id,omitempty"`
	Error        string           `json:"error,omitempty" bson:"error,omitempty"`
	Metrics      []TrainingMetric `json:"metrics" bson:"metrics"`
	StartedAt    *time.Time       `json:"started_at,omitempty" bson:"started_at,omitempty"`
	FinishedAt   *time.Time       `json:"finished_at,omitempty" bson:"finished_at,omitempty"`
	CreatedAt    time.Time        `json:"created_at" bson:"created_at"`
}

type TrainingMetric struct {
	Epoch         int       `json:"epoch" bson:"epoch"`
	Loss          float64   `json:"loss" bson:"loss"`
	RowsProcessed int       `json:"rows_processed" bson:"rows_processed"`
	CreatedAt     time.Time `json:"created_at" bson:"created_at"`
}

func Open(ctx context.Context, uri, database string) (*Store, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	store := &Store{client: client, db: client.Database(database)}
	if err := store.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return store, nil
}

func (s *Store) Close(ctx context.Context)      { _ = s.client.Disconnect(ctx) }
func (s *Store) Ping(ctx context.Context) error { return s.client.Ping(ctx, nil) }

func (s *Store) ensureIndexes(ctx context.Context) error {
	// Remove the index used by the previous relational-style document shape.
	// Ignore the error when it does not exist.
	_, _ = s.db.Collection("models").Indexes().DropOne(ctx, "version_1")
	indexes := map[string][]mongo.IndexModel{
		"forecasts": {
			{Keys: bson.D{{Key: "observed_at", Value: -1}}},
			{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}}},
			{Keys: bson.D{{Key: "class", Value: 1}, {Key: "created_at", Value: -1}}},
		},
		"models": {
			{Keys: bson.D{{Key: "model.version", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "active", Value: 1}}},
		},
		"training_runs":  {{Keys: bson.D{{Key: "created_at", Value: -1}}}},
		"cluster_events": {{Keys: bson.D{{Key: "created_at", Value: -1}}}},
	}
	for collection, models := range indexes {
		if _, err := s.db.Collection(collection).Indexes().CreateMany(ctx, models); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) EnsureInitialModel(ctx context.Context, model ml.Model) (ml.Model, error) {
	active, err := s.ActiveModel(ctx)
	if err == nil && active.Target == model.Target && len(active.Features) == len(model.Features) {
		return active, nil
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return ml.Model{}, err
	}
	if err == nil {
		_, _ = s.db.Collection("models").UpdateMany(ctx, bson.M{"active": true}, bson.M{"$set": bson.M{"active": false}})
	}
	model.ID = newID()
	if model.Version == "" {
		model.Version = "initial"
	}
	document := bson.M{"_id": model.ID, "model": model, "active": true, "created_at": time.Now().UTC()}
	_, err = s.db.Collection("models").InsertOne(ctx, document)
	return model, err
}

func (s *Store) ActiveModel(ctx context.Context) (ml.Model, error) {
	var document struct {
		Model ml.Model `bson:"model"`
	}
	err := s.db.Collection("models").FindOne(ctx, bson.M{"active": true}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ml.Model{}, ErrNotFound
	}
	return document.Model, err
}

func (s *Store) SaveForecast(ctx context.Context, record ForecastRecord) error {
	_, err := s.db.Collection("forecasts").InsertOne(ctx, record)
	return err
}

func (s *Store) Forecast(ctx context.Context, id, userID string) (ForecastRecord, error) {
	var record ForecastRecord
	filter := bson.M{"_id": id}
	if userID != "" {
		filter["user_id"] = userID
	}
	err := s.db.Collection("forecasts").FindOne(ctx, filter).Decode(&record)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ForecastRecord{}, ErrNotFound
	}
	return record, err
}

func (s *Store) ListForecasts(ctx context.Context, userID string, limit, offset int, class *int, from, to *time.Time) ([]ForecastRecord, error) {
	filter := bson.M{"user_id": userID}
	if class != nil {
		filter["class"] = *class
	}
	if from != nil || to != nil {
		date := bson.M{}
		if from != nil {
			date["$gte"] = *from
		}
		if to != nil {
			date["$lte"] = *to
		}
		filter["observed_at"] = date
	}
	cursor, err := s.db.Collection("forecasts").Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(int64(limit)).SetSkip(int64(offset)))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var records []ForecastRecord
	return records, cursor.All(ctx, &records)
}

func (s *Store) CreateTraining(ctx context.Context, epochs int, learningRate float64, workers int) (TrainingRun, error) {
	run := TrainingRun{
		ID: newID(), Status: "queued", Epochs: epochs, LearningRate: learningRate,
		Workers: workers, CreatedAt: time.Now().UTC(), Metrics: []TrainingMetric{},
	}
	_, err := s.db.Collection("training_runs").InsertOne(ctx, run)
	return run, err
}

func (s *Store) StartTraining(ctx context.Context, id string, workers int) error {
	now := time.Now().UTC()
	_, err := s.db.Collection("training_runs").UpdateOne(ctx, bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": "running", "workers": workers, "started_at": now}})
	return err
}

func (s *Store) SaveEpoch(ctx context.Context, runID string, metric cluster.EpochMetric) error {
	stored := TrainingMetric{Epoch: metric.Epoch, Loss: metric.Loss, RowsProcessed: metric.Rows, CreatedAt: time.Now().UTC()}
	_, err := s.db.Collection("training_runs").UpdateOne(ctx, bson.M{"_id": runID},
		bson.M{"$set": bson.M{"current_epoch": metric.Epoch}, "$push": bson.M{"metrics": stored}})
	return err
}

func (s *Store) CompleteTraining(ctx context.Context, runID string, model ml.Model) (ml.Model, error) {
	model.ID = newID()
	if _, err := s.db.Collection("models").InsertOne(ctx, bson.M{
		"_id": model.ID, "model": model, "active": false, "created_at": time.Now().UTC(),
	}); err != nil {
		return ml.Model{}, err
	}
	if _, err := s.db.Collection("models").UpdateMany(ctx, bson.M{"active": true}, bson.M{"$set": bson.M{"active": false}}); err != nil {
		return ml.Model{}, err
	}
	if _, err := s.db.Collection("models").UpdateOne(ctx, bson.M{"_id": model.ID}, bson.M{"$set": bson.M{"active": true}}); err != nil {
		return ml.Model{}, err
	}
	now := time.Now().UTC()
	_, err := s.db.Collection("training_runs").UpdateOne(ctx, bson.M{"_id": runID},
		bson.M{"$set": bson.M{"status": "completed", "model_id": model.ID, "current_epoch": model.Epochs, "finished_at": now}})
	return model, err
}

func (s *Store) FailTraining(ctx context.Context, id string, failure error) error {
	_, err := s.db.Collection("training_runs").UpdateOne(ctx, bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": "failed", "error": failure.Error(), "finished_at": time.Now().UTC()}})
	return err
}

func (s *Store) Training(ctx context.Context, id string) (TrainingRun, error) {
	var run TrainingRun
	err := s.db.Collection("training_runs").FindOne(ctx, bson.M{"_id": id}).Decode(&run)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return TrainingRun{}, ErrNotFound
	}
	return run, err
}

func (s *Store) ListTrainings(ctx context.Context, limit, offset int) ([]TrainingRun, error) {
	cursor, err := s.db.Collection("training_runs").Find(ctx, bson.M{},
		options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(int64(limit)).SetSkip(int64(offset)))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var runs []TrainingRun
	return runs, cursor.All(ctx, &runs)
}

func (s *Store) SaveClusterEvent(ctx context.Context, event cluster.Event) {
	_, _ = s.db.Collection("cluster_events").InsertOne(ctx, bson.M{
		"node_id": event.NodeID, "event_type": event.Type, "details": event.Details, "created_at": time.Now().UTC(),
	})
}

func newID() string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(value[:])
}
