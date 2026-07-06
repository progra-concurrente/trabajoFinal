package dataset

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type byteRange struct {
	Index int
	Start int64
	End   int64
}

func LoadData(root string, workers, total, chunkSize int) {
	var wg sync.WaitGroup
	stageDurations := make([]stageDuration, 0, 7)

	if workers <= 0 {
		workers = 1
	}

	rawDir := filepath.Join(root, "data", "raw", "household_power_consumption.txt")
	fileInfo, err := os.Stat(rawDir)
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		return
	}

	stageStart := time.Now()
	jobs := buildByteJobs(fileInfo.Size(), workers)
	jobChan := make(chan byteRange, len(jobs))
	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)

	resultChan := make(chan chunkResult, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobChan {
				chunkData, err := loadChunk(rawDir, job.Start, job.End)
				if err != nil {
					fmt.Printf("Error loading chunk: %v\n", err)
					continue
				}
				chunkData.Index = job.Index
				resultChan <- chunkData
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	orderedChunks := make([][]PowerConsumption, len(jobs))
	var stats LoadStats
	for result := range resultChan {
		orderedChunks[result.Index] = result.Records
		stats.Add(result.Stats)
	}

	wg.Wait()
	cleanData := make([]PowerConsumption, 0, total)
	for _, records := range orderedChunks {
		cleanData = mergeChunks(cleanData, records)
	}
	stageDurations = append(stageDurations, stageDuration{
		Name:     "Carga y limpieza paralela",
		Duration: time.Since(stageStart),
	})

	stageStart = time.Now()
	outputDir := filepath.Join(root, "data", "processed")
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return
	}
	outputFile := filepath.Join(outputDir, "processed_data.csv")
	if err := saveToCSV(outputFile, cleanData); err != nil {
		fmt.Printf("Error saving to CSV: %v\n", err)
		return
	}
	stageDurations = append(stageDurations, stageDuration{
		Name:     "Escritura del dataset limpio",
		Duration: time.Since(stageStart),
	})

	stageStart = time.Now()
	demandAnalysis := analyzeDemand(cleanData)
	report := buildSustainabilityReportFromAnalysis(demandAnalysis)
	stageDurations = append(stageDurations, stageDuration{
		Name:     "Analisis de demanda y sostenibilidad",
		Duration: time.Since(stageStart),
	})

	stageStart = time.Now()
	if err := saveAggregatesCSV(filepath.Join(outputDir, "hourly_demand.csv"), demandAnalysis.Hourly); err != nil {
		fmt.Printf("Error saving hourly demand analysis: %v\n", err)
		return
	}
	if err := saveAggregatesCSV(filepath.Join(outputDir, "daily_demand.csv"), demandAnalysis.Daily); err != nil {
		fmt.Printf("Error saving daily demand analysis: %v\n", err)
		return
	}
	if err := saveAggregatesCSV(filepath.Join(outputDir, "monthly_demand.csv"), demandAnalysis.Monthly); err != nil {
		fmt.Printf("Error saving monthly demand analysis: %v\n", err)
		return
	}
	if err := saveCombinedAggregatesCSV(filepath.Join(outputDir, "day_hour_demand.csv"), demandAnalysis.DayHour); err != nil {
		fmt.Printf("Error saving day-hour demand analysis: %v\n", err)
		return
	}
	if err := saveAggregatesCSV(filepath.Join(outputDir, "calendar_date_demand.csv"), demandAnalysis.CalendarDate); err != nil {
		fmt.Printf("Error saving calendar-date demand analysis: %v\n", err)
		return
	}
	if err := saveCombinedAggregatesCSV(filepath.Join(outputDir, "month_hour_demand.csv"), demandAnalysis.MonthHour); err != nil {
		fmt.Printf("Error saving month-hour demand analysis: %v\n", err)
		return
	}
	if err := saveSustainabilityReportJSON(filepath.Join(outputDir, "sustainability_report.json"), report); err != nil {
		fmt.Printf("Error saving sustainability report: %v\n", err)
		return
	}
	stageDurations = append(stageDurations, stageDuration{
		Name:     "Escritura de reportes de analisis",
		Duration: time.Since(stageStart),
	})

	stageStart = time.Now()
	forecastRecords := buildForecastRecords(cleanData, workers)
	forecastTraining, forecastTest := splitForecastTrainTest(forecastRecords, modelTrainRatio)
	if err := saveForecastCSV(filepath.Join(outputDir, "forecast_training.csv"), forecastTraining); err != nil {
		fmt.Printf("Error saving forecast training dataset: %v\n", err)
		return
	}
	if err := saveForecastCSV(filepath.Join(outputDir, "forecast_test.csv"), forecastTest); err != nil {
		fmt.Printf("Error saving forecast test dataset: %v\n", err)
		return
	}
	stageDurations = append(stageDurations, stageDuration{
		Name: "Construccion concurrente de ventanas futuras", Duration: time.Since(stageStart),
	})

	stageStart = time.Now()
	model := trainForecastModel(forecastTraining, forecastTest, workers)
	stageDurations = append(stageDurations, stageDuration{
		Name:     "Entrenamiento predictivo y evaluacion paralelos",
		Duration: time.Since(stageStart),
	})

	stageStart = time.Now()
	modelFile := filepath.Join(outputDir, "forecast_model.json")
	if err := saveModelJSON(modelFile, model); err != nil {
		fmt.Printf("Error saving ML model: %v\n", err)
		return
	}
	stageDurations = append(stageDurations, stageDuration{
		Name:     "Escritura del modelo ML",
		Duration: time.Since(stageStart),
	})
	pipelineMetrics := buildPipelineMetrics(workers, stats, len(forecastRecords), model, stageDurations)
	if err := savePipelineMetricsJSON(filepath.Join(outputDir, "pipeline_metrics.json"), pipelineMetrics); err != nil {
		fmt.Printf("Error saving pipeline metrics: %v\n", err)
		return
	}

	fmt.Printf("Rows read: %d\n", stats.LinesRead)
	fmt.Printf("Rows clean: %d\n", stats.RowsClean)
	fmt.Printf("Rows dropped by missing values: %d\n", stats.DroppedMissing)
	fmt.Printf("Rows dropped by invalid values: %d\n", stats.DroppedInvalid)
	fmt.Printf("ML model: %s\n", model.ModelType)
	fmt.Printf("ML parallel workers: %d\n", model.Workers)
	fmt.Printf("ML objective: forecast sustained high consumption in the next %d minutes\n", model.HorizonMinutes)
	fmt.Printf("ML split: %s (train=%d, test=%d)\n", model.SplitStrategy, model.TrainRows, model.TestRows)
	printClassificationMetrics("Training metrics", model.TrainingMetrics)
	printClassificationMetrics("Test metrics", model.TestMetrics)
	fmt.Printf("Sustainability report: %s\n", filepath.Join(outputDir, "sustainability_report.json"))
	fmt.Printf("Pipeline metrics: %s\n", filepath.Join(outputDir, "pipeline_metrics.json"))
	printTimingSummary(stageDurations)
}

func printClassificationMetrics(title string, metrics ClassificationMetrics) {
	fmt.Printf("\n%s:\n", title)
	fmt.Printf("- Accuracy:          %.4f\n", metrics.Accuracy)
	fmt.Printf("- Precision:         %.4f\n", metrics.Precision)
	fmt.Printf("- Recall:            %.4f\n", metrics.Recall)
	fmt.Printf("- Specificity:       %.4f\n", metrics.Specificity)
	fmt.Printf("- F1 score:          %.4f\n", metrics.F1Score)
	fmt.Printf("- Balanced accuracy: %.4f\n", metrics.BalancedAccuracy)
	fmt.Printf("- Positive rate:     %.4f\n", metrics.PositiveRate)
	fmt.Printf("- Loss:              %.4f\n", metrics.Loss)
	fmt.Printf("- Confusion matrix:  TP=%d TN=%d FP=%d FN=%d\n",
		metrics.TruePositive,
		metrics.TrueNegative,
		metrics.FalsePositive,
		metrics.FalseNegative,
	)
}

type stageDuration struct {
	Name     string
	Duration time.Duration
}

func printTimingSummary(stages []stageDuration) {
	var total time.Duration

	fmt.Println("\nMetricas de tiempo:")
	for _, stage := range stages {
		total += stage.Duration
		fmt.Printf("- %-40s %12.3f ms\n", stage.Name+":", durationMilliseconds(stage.Duration))
	}
	fmt.Printf("- %-40s %12.3f ms\n", "TOTAL (suma de etapas):", durationMilliseconds(total))
}

func buildPipelineMetrics(workers int, stats LoadStats, forecastRows int, model LogisticModel, stages []stageDuration) PipelineMetrics {
	metrics := PipelineMetrics{
		GeneratedAt:    time.Now().UTC(),
		Workers:        workers,
		RowsRead:       stats.LinesRead,
		RowsClean:      stats.RowsClean,
		DroppedMissing: stats.DroppedMissing,
		DroppedInvalid: stats.DroppedInvalid,
		ForecastRows:   forecastRows,
		TrainRows:      model.TrainRows,
		TestRows:       model.TestRows,
		Stages:         make([]PipelineStageMetric, 0, len(stages)),
	}
	for _, stage := range stages {
		duration := durationMilliseconds(stage.Duration)
		metrics.Stages = append(metrics.Stages, PipelineStageMetric{Name: stage.Name, DurationMS: duration})
		metrics.TotalDurationMS += duration
	}
	return metrics
}

func durationMilliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func loadChunk(filePath string, start, end int64) (chunkResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return chunkResult{}, err
	}
	defer file.Close()

	position := start
	var reader *bufio.Reader
	if start == 0 {
		reader = bufio.NewReader(file)
		header, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return chunkResult{}, err
			}
			return chunkResult{}, nil
		}
		position += int64(len(header))
	} else {
		if _, err := file.Seek(start-1, io.SeekStart); err != nil {
			return chunkResult{}, err
		}
		previousByte := make([]byte, 1)
		if _, err := io.ReadFull(file, previousByte); err != nil {
			return chunkResult{}, err
		}

		reader = bufio.NewReader(file)
		if previousByte[0] != '\n' {
			skipped, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return chunkResult{}, err
			}
			position += int64(len(skipped))
		}
	}

	var chunk []PowerConsumption
	var stats LoadStats
	for position < end {
		line, err := reader.ReadString('\n')
		if len(line) == 0 && err == io.EOF {
			break
		}
		if len(line) > 0 {
			position += int64(len(line))
			line = strings.TrimRight(line, "\r\n")
			if line != "" {
				row, rowStats := processLine(line)
				if rowStats.RowsClean > 0 {
					chunk = append(chunk, row)
				}
				stats.Add(rowStats)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return chunkResult{}, err
		}
	}
	return chunkResult{Records: chunk, Stats: stats}, nil
}

func mergeChunks(base, incoming []PowerConsumption) []PowerConsumption {
	for _, inc := range incoming {
		base = append(base, inc)
	}
	return base
}

func buildByteJobs(fileSize int64, workers int) []byteRange {
	if workers <= 0 {
		workers = 1
	}
	blockSize := fileSize / int64(workers)
	jobs := make([]byteRange, 0, workers)
	for i := 0; i < workers; i++ {
		start := int64(i) * blockSize
		end := start + blockSize
		if i == workers-1 {
			end = fileSize
		}
		jobs = append(jobs, byteRange{Index: i, Start: start, End: end})
	}
	return jobs
}

func processLine(line string) (PowerConsumption, LoadStats) {
	parsed, err := parseLine(line)
	valid := err == nil && validateRecord(parsed)

	var stats LoadStats
	analyzeLine(&stats, parsed, err, valid)

	if !valid {
		return PowerConsumption{}, stats
	}

	return transformRecord(parsed), stats
}
