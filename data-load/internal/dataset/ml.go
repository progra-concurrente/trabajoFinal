package dataset

import (
	"math"
	"sync"

	sharedml "powersight/pkg/ml"
)

const (
	logisticEpochs         = 80
	logisticLearningRate   = 0.25
	modelTrainRatio        = 0.80
	modelDecisionThreshold = 0.50
)

var logisticFeatureNames = append([]string(nil), sharedml.FeatureNames...)

type classificationCounts struct {
	truePositive, trueNegative, falsePositive, falseNegative int
	loss                                                     float64
	count, positive                                          int
}

func splitForecastTrainTest(records []sharedml.Record, trainRatio float64) ([]sharedml.Record, []sharedml.Record) {
	if len(records) < 2 {
		return records, nil
	}
	if trainRatio <= 0 || trainRatio >= 1 {
		trainRatio = modelTrainRatio
	}
	index := int(float64(len(records)) * trainRatio)
	if index < 1 {
		index = 1
	}
	if index >= len(records) {
		index = len(records) - 1
	}
	return records[:index], records[index:]
}

func trainForecastModel(training, test []sharedml.Record, workers int) LogisticModel {
	if workers < 1 {
		workers = 1
	}
	weights := make([]float64, len(logisticFeatureNames)+1)
	for epoch := 0; epoch < logisticEpochs; epoch++ {
		partials := computeGradientsParallel(training, weights, workers)
		gradient, err := sharedml.AggregateGradients(partials, len(weights))
		if err != nil {
			break
		}
		weights = sharedml.ApplyGradient(weights, gradient, logisticLearningRate)
	}
	decisionThreshold := chooseDecisionThreshold(training, weights, workers)
	trainingMetrics := computeForecastMetricsParallel(training, weights, workers, decisionThreshold)
	testMetrics := computeForecastMetricsParallel(test, weights, workers, decisionThreshold)
	total := len(training) + len(test)
	return LogisticModel{
		ModelType: "logistic_regression", Target: "FutureSustainedHighConsumption30m",
		Features: logisticFeatureNames, Weights: weights,
		LearningRate: logisticLearningRate, Epochs: logisticEpochs, Workers: workers,
		Rows: total, TrainRows: len(training), TestRows: len(test),
		TrainRatio: safeRatio(len(training), total), SplitStrategy: "temporal_80_20",
		Threshold: highConsumptionThreshold, DecisionThreshold: decisionThreshold,
		HistoryMinutes: forecastHistoryMinutes, HorizonMinutes: forecastHorizonMinutes,
		SustainedMinutes: forecastSustainedMinutes,
		Accuracy:         testMetrics.Accuracy, Loss: testMetrics.Loss,
		TrainingMetrics: trainingMetrics, TestMetrics: testMetrics,
	}
}

func computeGradientsParallel(records []sharedml.Record, weights []float64, workers int) []sharedml.Gradient {
	if len(records) == 0 {
		return nil
	}
	if workers > len(records) {
		workers = len(records)
	}
	results := make(chan sharedml.Gradient, workers)
	var wg sync.WaitGroup
	for _, job := range buildIndexJobs(len(records), workers) {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			results <- sharedml.ComputeGradient(records[start:end], weights)
		}(job.Start, job.End)
	}
	go func() { wg.Wait(); close(results) }()
	partials := make([]sharedml.Gradient, 0, workers)
	for result := range results {
		partials = append(partials, result)
	}
	return partials
}

func chooseDecisionThreshold(records []sharedml.Record, weights []float64, workers int) float64 {
	bestThreshold, bestF2 := modelDecisionThreshold, -1.0
	for threshold := 0.20; threshold <= 0.70; threshold += 0.05 {
		metrics := computeForecastMetricsParallel(records, weights, workers, threshold)
		f2 := safeFloatRatio(5*metrics.Precision*metrics.Recall, 4*metrics.Precision+metrics.Recall)
		if f2 > bestF2 {
			bestThreshold, bestF2 = threshold, f2
		}
	}
	return bestThreshold
}

func computeForecastMetricsParallel(records []sharedml.Record, weights []float64, workers int, threshold float64) ClassificationMetrics {
	if len(records) == 0 {
		return ClassificationMetrics{}
	}
	if workers > len(records) {
		workers = len(records)
	}
	results := make(chan classificationCounts, workers)
	var wg sync.WaitGroup
	model := sharedml.Model{Weights: weights, DecisionThreshold: threshold}
	for _, job := range buildIndexJobs(len(records), workers) {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			var counts classificationCounts
			for _, record := range records[start:end] {
				prediction, _ := sharedml.Predict(model, record.ForecastFeatures)
				switch {
				case prediction.Class == 1 && record.FutureHighConsumption == 1:
					counts.truePositive++
				case prediction.Class == 0 && record.FutureHighConsumption == 0:
					counts.trueNegative++
				case prediction.Class == 1 && record.FutureHighConsumption == 0:
					counts.falsePositive++
				case prediction.Class == 0 && record.FutureHighConsumption == 1:
					counts.falseNegative++
				}
				counts.loss += logisticLoss(float64(record.FutureHighConsumption), prediction.Probability)
				counts.count++
				counts.positive += record.FutureHighConsumption
			}
			results <- counts
		}(job.Start, job.End)
	}
	go func() { wg.Wait(); close(results) }()
	var total classificationCounts
	for current := range results {
		total.truePositive += current.truePositive
		total.trueNegative += current.trueNegative
		total.falsePositive += current.falsePositive
		total.falseNegative += current.falseNegative
		total.loss += current.loss
		total.count += current.count
		total.positive += current.positive
	}
	precision := safeRatio(total.truePositive, total.truePositive+total.falsePositive)
	recall := safeRatio(total.truePositive, total.truePositive+total.falseNegative)
	specificity := safeRatio(total.trueNegative, total.trueNegative+total.falsePositive)
	return ClassificationMetrics{
		Rows: total.count, TruePositive: total.truePositive, TrueNegative: total.trueNegative,
		FalsePositive: total.falsePositive, FalseNegative: total.falseNegative,
		Accuracy:  safeRatio(total.truePositive+total.trueNegative, total.count),
		Precision: precision, Recall: recall, Specificity: specificity,
		F1Score:          safeFloatRatio(2*precision*recall, precision+recall),
		BalancedAccuracy: (recall + specificity) / 2, PositiveRate: safeRatio(total.positive, total.count),
		Loss: safeFloatRatio(total.loss, float64(total.count)),
	}
}

func logisticLoss(label, prediction float64) float64 {
	const epsilon = 1e-12
	prediction = math.Max(epsilon, math.Min(1-epsilon, prediction))
	return -(label*math.Log(prediction) + (1-label)*math.Log(1-prediction))
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

type indexRange struct{ Start, End int }

func buildIndexJobs(length, workers int) []indexRange {
	if workers < 1 {
		workers = 1
	}
	if workers > length {
		workers = length
	}
	block, remainder, start := length/workers, length%workers, 0
	jobs := make([]indexRange, 0, workers)
	for index := 0; index < workers; index++ {
		end := start + block
		if index < remainder {
			end++
		}
		jobs = append(jobs, indexRange{start, end})
		start = end
	}
	return jobs
}
