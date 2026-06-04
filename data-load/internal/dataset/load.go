package dataset

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type byteRange struct {
	Start int64
	End   int64
}

func LoadData(root string, workers, total, chunkSize int) {
	var wg sync.WaitGroup

	if workers <= 0 {
		workers = 1
	}

	rawDir := filepath.Join(root, "data", "raw", "household_power_consumption.txt")
	fileInfo, err := os.Stat(rawDir)
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		return
	}

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
				resultChan <- chunkData
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	cleanData := make([]PowerConsumption, 0, total)
	var stats LoadStats
	for result := range resultChan {
		cleanData = mergeChunks(cleanData, result.Records)
		stats.Add(result.Stats)
	}

	wg.Wait()

	//load data into csv
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

	hourlyDemand := aggregateBy(cleanData, func(record PowerConsumption) string {
		return fmt.Sprintf("%02d:00", record.Hour)
	})
	dailyDemand := aggregateBy(cleanData, func(record PowerConsumption) string {
		return dayName(record.DayOfWeek)
	})
	monthlyDemand := aggregateBy(cleanData, func(record PowerConsumption) string {
		return monthName(record.Month)
	})
	if err := saveAggregatesCSV(filepath.Join(outputDir, "hourly_demand.csv"), hourlyDemand); err != nil {
		fmt.Printf("Error saving hourly demand analysis: %v\n", err)
		return
	}
	if err := saveAggregatesCSV(filepath.Join(outputDir, "daily_demand.csv"), dailyDemand); err != nil {
		fmt.Printf("Error saving daily demand analysis: %v\n", err)
		return
	}
	if err := saveAggregatesCSV(filepath.Join(outputDir, "monthly_demand.csv"), monthlyDemand); err != nil {
		fmt.Printf("Error saving monthly demand analysis: %v\n", err)
		return
	}
	report := buildSustainabilityReport(cleanData)
	if err := saveSustainabilityReportJSON(filepath.Join(outputDir, "sustainability_report.json"), report); err != nil {
		fmt.Printf("Error saving sustainability report: %v\n", err)
		return
	}

	model := trainLogisticModel(cleanData, workers)
	modelFile := filepath.Join(outputDir, "logistic_model.json")
	if err := saveModelJSON(modelFile, model); err != nil {
		fmt.Printf("Error saving ML model: %v\n", err)
		return
	}

	fmt.Printf("Rows read: %d\n", stats.LinesRead)
	fmt.Printf("Rows clean: %d\n", stats.RowsClean)
	fmt.Printf("Rows dropped by missing values: %d\n", stats.DroppedMissing)
	fmt.Printf("Rows dropped by invalid values: %d\n", stats.DroppedInvalid)
	fmt.Printf("ML model: %s\n", model.ModelType)
	fmt.Printf("ML parallel workers: %d\n", model.Workers)
	fmt.Printf("ML accuracy: %.4f\n", model.Accuracy)
	fmt.Printf("ML loss: %.4f\n", model.Loss)
	fmt.Printf("Sustainability report: %s\n", filepath.Join(outputDir, "sustainability_report.json"))
}

func loadChunk(filePath string, start, end int64) (chunkResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return chunkResult{}, err
	}
	defer file.Close()

	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return chunkResult{}, err
	}

	reader := bufio.NewReader(file)
	bytesRead := int64(0)
	limit := end - start
	if limit < 0 {
		limit = 0
	}

	if start == 0 {
		if _, err := reader.ReadString('\n'); err != nil {
			if err != io.EOF {
				return chunkResult{}, err
			}
			return chunkResult{}, nil
		}
	} else {
		skipped, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return chunkResult{}, err
		}
		bytesRead += int64(len(skipped))
	}

	var chunk []PowerConsumption
	var stats LoadStats
	for bytesRead < limit {
		line, err := reader.ReadString('\n')
		if len(line) == 0 && err == io.EOF {
			break
		}
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if line != "" {
				row, rowStats := processLine(line)
				if rowStats.RowsClean > 0 {
					chunk = append(chunk, row)
				}
				stats.Add(rowStats)
			}
			bytesRead += int64(len(line))
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
		jobs = append(jobs, byteRange{Start: start, End: end})
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
