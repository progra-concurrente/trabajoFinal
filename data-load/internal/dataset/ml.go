package dataset

import (
	"math"
	"sync"
)

const (
	logisticEpochs         = 80
	logisticLearningRate   = 0.25
	modelTrainRatio        = 0.80
	modelDecisionThreshold = 0.50
)

var logisticFeatureNames = []string{
	"GlobalReactivePower",
	"Voltage",
	"GlobalIntensity",
	"SubMeteringTotal",
	"OtherConsumption",
	"Hour",
	"DayOfWeek",
	"Month",
}

type logisticGradient struct {
	Values []float64
	Loss   float64
	Count  int
}

type classificationCounts struct {
	TruePositive  int
	TrueNegative  int
	FalsePositive int
	FalseNegative int
	Loss          float64
	Count         int
	Positive      int
}

func splitTrainTest(records []PowerConsumption, trainRatio float64) ([]PowerConsumption, []PowerConsumption) {
	if len(records) == 0 {
		return nil, nil
	}
	if trainRatio <= 0 || trainRatio >= 1 {
		trainRatio = modelTrainRatio
	}

	splitIndex := int(float64(len(records)) * trainRatio)
	if splitIndex < 1 {
		splitIndex = 1
	}
	if splitIndex >= len(records) {
		splitIndex = len(records) - 1
	}
	return records[:splitIndex], records[splitIndex:]
}

func trainLogisticModel(trainingRecords, testRecords []PowerConsumption, workers int) LogisticModel {
	if workers <= 0 {
		workers = 1
	}
	if len(trainingRecords) == 0 {
		return LogisticModel{
			ModelType:         "logistic_regression",
			Target:            "HighConsumption",
			Features:          logisticFeatureNames,
			Weights:           make([]float64, len(logisticFeatureNames)+1),
			LearningRate:      logisticLearningRate,
			Epochs:            logisticEpochs,
			Workers:           workers,
			Threshold:         highConsumptionThreshold,
			DecisionThreshold: modelDecisionThreshold,
			TrainRatio:        modelTrainRatio,
			SplitStrategy:     "temporal_80_20",
		}
	}

	weights := make([]float64, len(logisticFeatureNames)+1)
	for epoch := 0; epoch < logisticEpochs; epoch++ {
		gradient, _ := computeGradientParallel(trainingRecords, weights, workers)
		for i := range weights {
			weights[i] -= logisticLearningRate * gradient[i]
		}
	}

	trainingMetrics := computeClassificationMetricsParallel(trainingRecords, weights, workers)
	testMetrics := computeClassificationMetricsParallel(testRecords, weights, workers)
	totalRows := len(trainingRecords) + len(testRecords)
	return LogisticModel{
		ModelType:         "logistic_regression",
		Target:            "HighConsumption",
		Features:          logisticFeatureNames,
		Weights:           weights,
		LearningRate:      logisticLearningRate,
		Epochs:            logisticEpochs,
		Workers:           workers,
		Rows:              totalRows,
		TrainRows:         len(trainingRecords),
		TestRows:          len(testRecords),
		TrainRatio:        safeRatio(len(trainingRecords), totalRows),
		SplitStrategy:     "temporal_80_20",
		Threshold:         highConsumptionThreshold,
		DecisionThreshold: modelDecisionThreshold,
		Accuracy:          testMetrics.Accuracy,
		Loss:              testMetrics.Loss,
		TrainingMetrics:   trainingMetrics,
		TestMetrics:       testMetrics,
	}
}

func computeGradientParallel(records []PowerConsumption, weights []float64, workers int) ([]float64, float64) {
	if workers <= 0 {
		workers = 1
	}
	if workers > len(records) {
		workers = len(records)
	}

	results := make(chan logisticGradient, workers)
	var wg sync.WaitGroup
	for _, job := range buildIndexJobs(len(records), workers) {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			results <- computeGradient(records[start:end], weights)
		}(job.Start, job.End)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	gradient := make([]float64, len(weights))
	loss := 0.0
	total := 0
	for result := range results {
		for i, value := range result.Values {
			gradient[i] += value
		}
		loss += result.Loss
		total += result.Count
	}

	if total == 0 {
		return gradient, 0
	}
	for i := range gradient {
		gradient[i] /= float64(total)
	}
	return gradient, loss / float64(total)
}

func computeGradient(records []PowerConsumption, weights []float64) logisticGradient {
	gradient := make([]float64, len(weights))
	loss := 0.0

	for _, record := range records {
		features := featureVector(record)
		prediction := sigmoid(dot(weights, features))
		label := float64(record.HighConsumption)
		errorValue := prediction - label

		for i := range gradient {
			gradient[i] += errorValue * features[i]
		}
		loss += logisticLoss(label, prediction)
	}

	return logisticGradient{
		Values: gradient,
		Loss:   loss,
		Count:  len(records),
	}
}

func computeClassificationMetricsParallel(records []PowerConsumption, weights []float64, workers int) ClassificationMetrics {
	if len(records) == 0 {
		return ClassificationMetrics{}
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(records) {
		workers = len(records)
	}

	results := make(chan classificationCounts, workers)
	var wg sync.WaitGroup
	for _, job := range buildIndexJobs(len(records), workers) {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			var counts classificationCounts
			for _, record := range records[start:end] {
				prediction := sigmoid(dot(weights, featureVector(record)))
				label := 0
				if prediction >= modelDecisionThreshold {
					label = 1
				}
				switch {
				case label == 1 && record.HighConsumption == 1:
					counts.TruePositive++
				case label == 0 && record.HighConsumption == 0:
					counts.TrueNegative++
				case label == 1 && record.HighConsumption == 0:
					counts.FalsePositive++
				case label == 0 && record.HighConsumption == 1:
					counts.FalseNegative++
				}
				counts.Loss += logisticLoss(float64(record.HighConsumption), prediction)
				counts.Count++
				counts.Positive += record.HighConsumption
			}
			results <- counts
		}(job.Start, job.End)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var total classificationCounts
	for result := range results {
		total.TruePositive += result.TruePositive
		total.TrueNegative += result.TrueNegative
		total.FalsePositive += result.FalsePositive
		total.FalseNegative += result.FalseNegative
		total.Loss += result.Loss
		total.Count += result.Count
		total.Positive += result.Positive
	}

	precision := safeRatio(total.TruePositive, total.TruePositive+total.FalsePositive)
	recall := safeRatio(total.TruePositive, total.TruePositive+total.FalseNegative)
	specificity := safeRatio(total.TrueNegative, total.TrueNegative+total.FalsePositive)
	return ClassificationMetrics{
		Rows:             total.Count,
		TruePositive:     total.TruePositive,
		TrueNegative:     total.TrueNegative,
		FalsePositive:    total.FalsePositive,
		FalseNegative:    total.FalseNegative,
		Accuracy:         safeRatio(total.TruePositive+total.TrueNegative, total.Count),
		Precision:        precision,
		Recall:           recall,
		Specificity:      specificity,
		F1Score:          safeFloatRatio(2*precision*recall, precision+recall),
		BalancedAccuracy: (recall + specificity) / 2,
		PositiveRate:     safeRatio(total.Positive, total.Count),
		Loss:             safeFloatRatio(total.Loss, float64(total.Count)),
	}
}

func safeRatio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func safeFloatRatio(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func featureVector(record PowerConsumption) []float64 {
	return []float64{
		1,
		record.GlobalReactivePower,
		record.Voltage / 250,
		record.GlobalIntensity / 25,
		record.SubMeteringTotal / 50,
		record.OtherConsumption / 50,
		float64(record.Hour) / 23,
		float64(record.DayOfWeek) / 6,
		float64(record.Month) / 12,
	}
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
	epsilon := 1e-12
	prediction = math.Max(epsilon, math.Min(1-epsilon, prediction))
	return -(label*math.Log(prediction) + (1-label)*math.Log(1-prediction))
}

type indexRange struct {
	Start int
	End   int
}

func buildIndexJobs(length, workers int) []indexRange {
	if workers <= 0 {
		workers = 1
	}
	if workers > length {
		workers = length
	}

	blockSize := length / workers
	remainder := length % workers
	jobs := make([]indexRange, 0, workers)
	start := 0
	for i := 0; i < workers; i++ {
		end := start + blockSize
		if i < remainder {
			end++
		}
		jobs = append(jobs, indexRange{Start: start, End: end})
		start = end
	}
	return jobs
}
