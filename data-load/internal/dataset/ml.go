package dataset

import (
	"math"
	"sync"
)

const (
	logisticEpochs       = 80
	logisticLearningRate = 0.25
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

func trainLogisticModel(records []PowerConsumption, workers int) LogisticModel {
	if workers <= 0 {
		workers = 1
	}
	if len(records) == 0 {
		return LogisticModel{
			ModelType:    "logistic_regression",
			Target:       "HighConsumption",
			Features:     logisticFeatureNames,
			Weights:      make([]float64, len(logisticFeatureNames)+1),
			LearningRate: logisticLearningRate,
			Epochs:       logisticEpochs,
			Workers:      workers,
			Threshold:    highConsumptionThreshold,
		}
	}

	weights := make([]float64, len(logisticFeatureNames)+1)
	loss := 0.0
	for epoch := 0; epoch < logisticEpochs; epoch++ {
		gradient, epochLoss := computeGradientParallel(records, weights, workers)
		for i := range weights {
			weights[i] -= logisticLearningRate * gradient[i]
		}
		loss = epochLoss
	}

	return LogisticModel{
		ModelType:    "logistic_regression",
		Target:       "HighConsumption",
		Features:     logisticFeatureNames,
		Weights:      weights,
		LearningRate: logisticLearningRate,
		Epochs:       logisticEpochs,
		Workers:      workers,
		Rows:         len(records),
		Threshold:    highConsumptionThreshold,
		Accuracy:     computeAccuracy(records, weights, workers),
		Loss:         loss,
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

func computeAccuracy(records []PowerConsumption, weights []float64, workers int) float64 {
	if len(records) == 0 {
		return 0
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(records) {
		workers = len(records)
	}

	results := make(chan int, workers)
	var wg sync.WaitGroup
	for _, job := range buildIndexJobs(len(records), workers) {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			correct := 0
			for _, record := range records[start:end] {
				prediction := sigmoid(dot(weights, featureVector(record)))
				label := 0
				if prediction >= 0.5 {
					label = 1
				}
				if label == record.HighConsumption {
					correct++
				}
			}
			results <- correct
		}(job.Start, job.End)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	correct := 0
	for value := range results {
		correct += value
	}
	return float64(correct) / float64(len(records))
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
