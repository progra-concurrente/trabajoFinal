package ml

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
)

var FeatureNames = []string{
	"CurrentActivePower",
	"RecentAverageActivePower",
	"RecentMaximumActivePower",
	"RecentStdDevActivePower",
	"RecentActivePowerTrend",
	"CurrentReactivePower",
	"CurrentVoltage",
	"CurrentIntensity",
	"CurrentSubMeteringTotal",
	"CurrentOtherConsumption",
	"Hour",
	"DayOfWeek",
	"Month",
	"HistoricalHourHighRate",
	"HistoricalDayHourHighRate",
}

type ForecastFeatures struct {
	CurrentActivePower        float64 `json:"current_active_power"`
	RecentAverageActivePower  float64 `json:"recent_average_active_power"`
	RecentMaximumActivePower  float64 `json:"recent_maximum_active_power"`
	RecentStdDevActivePower   float64 `json:"recent_stddev_active_power"`
	RecentActivePowerTrend    float64 `json:"recent_active_power_trend"`
	CurrentReactivePower      float64 `json:"current_reactive_power"`
	CurrentVoltage            float64 `json:"current_voltage"`
	CurrentIntensity          float64 `json:"current_intensity"`
	CurrentSubMeteringTotal   float64 `json:"current_sub_metering_total"`
	CurrentOtherConsumption   float64 `json:"current_other_consumption"`
	Hour                      int     `json:"hour"`
	DayOfWeek                 int     `json:"day_of_week"`
	Month                     int     `json:"month"`
	HistoricalHourHighRate    float64 `json:"historical_hour_high_rate"`
	HistoricalDayHourHighRate float64 `json:"historical_day_hour_high_rate"`
}

type Record struct {
	ForecastFeatures
	FutureHighConsumption int `json:"future_high_consumption"`
}

type Model struct {
	ID                string          `json:"id,omitempty" bson:"_id,omitempty"`
	Version           string          `json:"version,omitempty"`
	ModelType         string          `json:"model_type"`
	Target            string          `json:"target"`
	Features          []string        `json:"features"`
	Weights           []float64       `json:"weights"`
	LearningRate      float64         `json:"learning_rate"`
	Epochs            int             `json:"epochs"`
	Threshold         float64         `json:"high_consumption_threshold"`
	DecisionThreshold float64         `json:"decision_threshold"`
	HistoryMinutes    int             `json:"history_minutes"`
	HorizonMinutes    int             `json:"horizon_minutes"`
	SustainedMinutes  int             `json:"sustained_minutes"`
	Workers           int             `json:"workers,omitempty"`
	Rows              int             `json:"rows,omitempty"`
	TrainRows         int             `json:"train_rows,omitempty"`
	TestRows          int             `json:"test_rows,omitempty"`
	TrainRatio        float64         `json:"train_ratio,omitempty"`
	SplitStrategy     string          `json:"split_strategy,omitempty"`
	Accuracy          float64         `json:"accuracy,omitempty"`
	Loss              float64         `json:"loss,omitempty"`
	TrainingMetrics   Metrics         `json:"training_metrics,omitempty"`
	TestMetrics       Metrics         `json:"test_metrics,omitempty"`
	Metrics           json.RawMessage `json:"metrics,omitempty"`
}

type Metrics struct {
	Rows             int     `json:"rows,omitempty"`
	TruePositive     int     `json:"true_positive,omitempty"`
	TrueNegative     int     `json:"true_negative,omitempty"`
	FalsePositive    int     `json:"false_positive,omitempty"`
	FalseNegative    int     `json:"false_negative,omitempty"`
	Accuracy         float64 `json:"accuracy,omitempty"`
	Precision        float64 `json:"precision,omitempty"`
	Recall           float64 `json:"recall,omitempty"`
	Specificity      float64 `json:"specificity,omitempty"`
	F1Score          float64 `json:"f1_score,omitempty"`
	BalancedAccuracy float64 `json:"balanced_accuracy,omitempty"`
	PositiveRate     float64 `json:"positive_rate,omitempty"`
	Loss             float64 `json:"loss,omitempty"`
}

// ComputeGradientConcurrent divides one node's shard among goroutines and
// combines partial gradients through a channel before returning it by TCP.
func ComputeGradientConcurrent(records []Record, weights []float64, workers int) Gradient {
	if len(records) == 0 {
		return Gradient{Values: make([]float64, len(weights))}
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(records) {
		workers = len(records)
	}
	results := make(chan Gradient, workers)
	block, remainder, start := len(records)/workers, len(records)%workers, 0
	for worker := 0; worker < workers; worker++ {
		end := start + block
		if worker < remainder {
			end++
		}
		chunk := records[start:end]
		go func() { results <- ComputeGradient(chunk, weights) }()
		start = end
	}
	partials := make([]Gradient, 0, workers)
	for worker := 0; worker < workers; worker++ {
		partials = append(partials, <-results)
	}
	average, err := AggregateGradients(partials, len(weights))
	if err != nil {
		return Gradient{Values: make([]float64, len(weights))}
	}
	for i := range average.Values {
		average.Values[i] *= float64(average.Count)
	}
	average.Loss *= float64(average.Count)
	return average
}

type Prediction struct {
	Probability float64 `json:"probability"`
	Class       int     `json:"class"`
}

type Gradient struct {
	Values []float64 `json:"values"`
	Loss   float64   `json:"loss"`
	Count  int       `json:"count"`
}

func ValidateFeatures(m ForecastFeatures) error {
	if m.CurrentVoltage <= 0 {
		return errors.New("voltage must be greater than zero")
	}
	if m.CurrentActivePower < 0 || m.CurrentReactivePower < 0 || m.CurrentIntensity < 0 || m.CurrentSubMeteringTotal < 0 {
		return errors.New("power, intensity, and sub-metering values cannot be negative")
	}
	if m.Hour < 0 || m.Hour > 23 {
		return errors.New("hour must be between 0 and 23")
	}
	if m.DayOfWeek < 0 || m.DayOfWeek > 6 {
		return errors.New("day_of_week must be between 0 and 6")
	}
	if m.Month < 1 || m.Month > 12 {
		return errors.New("month must be between 1 and 12")
	}
	if m.HistoricalHourHighRate < 0 || m.HistoricalHourHighRate > 1 ||
		m.HistoricalDayHourHighRate < 0 || m.HistoricalDayHourHighRate > 1 {
		return errors.New("historical rates must be between 0 and 1")
	}
	return nil
}

func FeatureVector(m ForecastFeatures) []float64 {
	return []float64{
		1,
		m.CurrentActivePower / 5,
		m.RecentAverageActivePower / 5,
		m.RecentMaximumActivePower / 10,
		m.RecentStdDevActivePower / 3,
		m.RecentActivePowerTrend / 5,
		m.CurrentReactivePower,
		m.CurrentVoltage / 250,
		m.CurrentIntensity / 25,
		m.CurrentSubMeteringTotal / 50,
		m.CurrentOtherConsumption / 50,
		float64(m.Hour) / 23,
		float64(m.DayOfWeek) / 6,
		float64(m.Month) / 12,
		m.HistoricalHourHighRate,
		m.HistoricalDayHourHighRate,
	}
}

func Predict(model Model, features ForecastFeatures) (Prediction, error) {
	if err := ValidateFeatures(features); err != nil {
		return Prediction{}, err
	}
	if len(model.Weights) != len(FeatureNames)+1 {
		return Prediction{}, errors.New("model has an invalid number of weights")
	}
	probability := sigmoid(dot(model.Weights, FeatureVector(features)))
	threshold := model.DecisionThreshold
	if threshold == 0 {
		threshold = 0.5
	}
	class := 0
	if probability >= threshold {
		class = 1
	}
	return Prediction{Probability: probability, Class: class}, nil
}

func ComputeGradient(records []Record, weights []float64) Gradient {
	values := make([]float64, len(weights))
	var loss float64
	for _, record := range records {
		features := FeatureVector(record.ForecastFeatures)
		prediction := sigmoid(dot(weights, features))
		label := float64(record.FutureHighConsumption)
		delta := prediction - label
		for i := range values {
			values[i] += delta * features[i]
		}
		loss += logisticLoss(label, prediction)
	}
	return Gradient{Values: values, Loss: loss, Count: len(records)}
}

func AggregateGradients(gradients []Gradient, weightCount int) (Gradient, error) {
	result := Gradient{Values: make([]float64, weightCount)}
	for _, gradient := range gradients {
		if len(gradient.Values) != weightCount {
			return Gradient{}, errors.New("gradient size does not match model")
		}
		for i, value := range gradient.Values {
			result.Values[i] += value
		}
		result.Loss += gradient.Loss
		result.Count += gradient.Count
	}
	if result.Count == 0 {
		return Gradient{}, errors.New("cannot aggregate empty gradients")
	}
	for i := range result.Values {
		result.Values[i] /= float64(result.Count)
	}
	result.Loss /= float64(result.Count)
	return result, nil
}

func ApplyGradient(weights []float64, gradient Gradient, learningRate float64) []float64 {
	next := append([]float64(nil), weights...)
	for i := range next {
		next[i] -= learningRate * gradient.Values[i]
	}
	return next
}

func LoadModel(path string) (Model, error) {
	file, err := os.Open(path)
	if err != nil {
		return Model{}, err
	}
	defer file.Close()
	var model Model
	err = json.NewDecoder(file).Decode(&model)
	if model.Version == "" {
		if model.Target == "FutureSustainedHighConsumption30m" {
			model.Version = "forecast-initial-80e"
		} else {
			model.Version = "legacy-initial"
		}
	}
	return model, err
}

// LoadCSVRange loads [start,end) data rows from the processed PowerSight CSV.
func LoadCSVRange(path string, start, end int) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		return nil, err
	}
	records := make([]Record, 0, max(0, end-start))
	for index := 0; ; index++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if index < start {
			continue
		}
		if end >= 0 && index >= end {
			break
		}
		record, err := parseCSVRecord(row)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func CountCSVRows(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		return 0, err
	}
	count := 0
	for {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return count, nil
			}
			return 0, err
		}
		count++
	}
}

func parseCSVRecord(row []string) (Record, error) {
	if len(row) < 17 {
		return Record{}, errors.New("forecast training row has fewer than 17 columns")
	}
	floatAt := func(index int) (float64, error) { return strconv.ParseFloat(row[index], 64) }
	intAt := func(index int) (int, error) { return strconv.Atoi(row[index]) }
	values := make([]float64, 0, 12)
	for index := 1; index <= 10; index++ {
		value, err := floatAt(index)
		if err != nil {
			return Record{}, err
		}
		values = append(values, value)
	}
	hour, err := intAt(11)
	if err != nil {
		return Record{}, err
	}
	day, err := intAt(12)
	if err != nil {
		return Record{}, err
	}
	month, err := intAt(13)
	if err != nil {
		return Record{}, err
	}
	hourRate, err := floatAt(14)
	if err != nil {
		return Record{}, err
	}
	dayHourRate, err := floatAt(15)
	if err != nil {
		return Record{}, err
	}
	label, err := intAt(16)
	if err != nil {
		return Record{}, err
	}
	return Record{
		ForecastFeatures: ForecastFeatures{
			CurrentActivePower: values[0], RecentAverageActivePower: values[1],
			RecentMaximumActivePower: values[2], RecentStdDevActivePower: values[3],
			RecentActivePowerTrend: values[4], CurrentReactivePower: values[5],
			CurrentVoltage: values[6], CurrentIntensity: values[7],
			CurrentSubMeteringTotal: values[8], CurrentOtherConsumption: values[9],
			Hour: hour, DayOfWeek: day, Month: month,
			HistoricalHourHighRate: hourRate, HistoricalDayHourHighRate: dayHourRate,
		},
		FutureHighConsumption: label,
	}, nil
}

func dot(weights, features []float64) float64 {
	total := 0.0
	for i := range weights {
		total += weights[i] * features[i]
	}
	return total
}

func sigmoid(value float64) float64 {
	if value >= 0 {
		z := math.Exp(-value)
		return 1 / (1 + z)
	}
	z := math.Exp(value)
	return z / (1 + z)
}

func logisticLoss(label, prediction float64) float64 {
	const epsilon = 1e-12
	prediction = math.Max(epsilon, math.Min(1-epsilon, prediction))
	return -(label*math.Log(prediction) + (1-label)*math.Log(1-prediction))
}
