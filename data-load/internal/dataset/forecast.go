package dataset

import (
	"fmt"
	"math"
	"sync"
	"time"

	sharedml "powersight/pkg/ml"
)

const (
	forecastHistoryMinutes   = 15
	forecastHorizonMinutes   = 30
	forecastSustainedMinutes = 10
	forecastSampleStride     = 5
)

type forecastRates struct {
	hour    map[int]float64
	dayHour map[string]float64
}

func buildForecastRecords(records []PowerConsumption, workers int) []sharedml.Record {
	if len(records) < forecastHistoryMinutes+forecastHorizonMinutes {
		return nil
	}
	if workers < 1 {
		workers = 1
	}
	rateLimit := int(float64(len(records)) * modelTrainRatio)
	rates := calculateForecastRates(records[:rateLimit])
	firstAnchor := forecastHistoryMinutes - 1
	lastAnchorExclusive := len(records) - forecastHorizonMinutes
	totalAnchors := (lastAnchorExclusive - firstAnchor + forecastSampleStride - 1) / forecastSampleStride
	if workers > totalAnchors {
		workers = totalAnchors
	}

	type job struct{ index, start, end int }
	type result struct {
		index   int
		records []sharedml.Record
	}
	jobs := make(chan job, workers)
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for current := range jobs {
				chunk := make([]sharedml.Record, 0, current.end-current.start)
				for ordinal := current.start; ordinal < current.end; ordinal++ {
					anchor := firstAnchor + ordinal*forecastSampleStride
					if anchor >= lastAnchorExclusive {
						break
					}
					if record, ok := buildForecastRecord(records, anchor, rates); ok {
						chunk = append(chunk, record)
					}
				}
				results <- result{index: current.index, records: chunk}
			}
		}()
	}
	block, remainder, start := totalAnchors/workers, totalAnchors%workers, 0
	for index := 0; index < workers; index++ {
		end := start + block
		if index < remainder {
			end++
		}
		jobs <- job{index: index, start: start, end: end}
		start = end
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	ordered := make([][]sharedml.Record, workers)
	total := 0
	for current := range results {
		ordered[current.index] = current.records
		total += len(current.records)
	}
	forecast := make([]sharedml.Record, 0, total)
	for _, chunk := range ordered {
		forecast = append(forecast, chunk...)
	}
	return forecast
}

func buildForecastRecord(records []PowerConsumption, anchor int, rates forecastRates) (sharedml.Record, bool) {
	historyStart := anchor - forecastHistoryMinutes + 1
	futureEnd := anchor + forecastHorizonMinutes
	for index := historyStart + 1; index <= futureEnd; index++ {
		if records[index].Timestamp.Sub(records[index-1].Timestamp) != time.Minute {
			return sharedml.Record{}, false
		}
	}
	var sum, maxValue float64
	maxValue = records[historyStart].GlobalActivePower
	for index := historyStart; index <= anchor; index++ {
		value := records[index].GlobalActivePower
		sum += value
		if value > maxValue {
			maxValue = value
		}
	}
	average := sum / forecastHistoryMinutes
	var variance float64
	for index := historyStart; index <= anchor; index++ {
		delta := records[index].GlobalActivePower - average
		variance += delta * delta
	}
	current := records[anchor]
	futureHigh := 0
	for index := anchor + 1; index <= futureEnd; index++ {
		if records[index].HighConsumption == 1 {
			futureHigh++
		}
	}
	label := 0
	if futureHigh >= forecastSustainedMinutes {
		label = 1
	}
	return sharedml.Record{
		ForecastFeatures: sharedml.ForecastFeatures{
			CurrentActivePower:       current.GlobalActivePower,
			RecentAverageActivePower: average,
			RecentMaximumActivePower: maxValue,
			RecentStdDevActivePower:  math.Sqrt(variance / forecastHistoryMinutes),
			RecentActivePowerTrend:   (current.GlobalActivePower - records[historyStart].GlobalActivePower) / (forecastHistoryMinutes - 1),
			CurrentReactivePower:     current.GlobalReactivePower,
			CurrentVoltage:           current.Voltage,
			CurrentIntensity:         current.GlobalIntensity,
			CurrentSubMeteringTotal:  current.SubMeteringTotal,
			CurrentOtherConsumption:  current.OtherConsumption,
			Hour:                     current.Hour, DayOfWeek: current.DayOfWeek, Month: current.Month,
			HistoricalHourHighRate:    rates.hour[current.Hour],
			HistoricalDayHourHighRate: rates.dayHour[fmt.Sprintf("%d-%d", current.DayOfWeek, current.Hour)],
		},
		FutureHighConsumption: label,
	}, true
}

func calculateForecastRates(records []PowerConsumption) forecastRates {
	type counter struct{ high, total int }
	hours := make(map[int]*counter)
	dayHours := make(map[string]*counter)
	for _, record := range records {
		if hours[record.Hour] == nil {
			hours[record.Hour] = &counter{}
		}
		key := fmt.Sprintf("%d-%d", record.DayOfWeek, record.Hour)
		if dayHours[key] == nil {
			dayHours[key] = &counter{}
		}
		hours[record.Hour].total++
		hours[record.Hour].high += record.HighConsumption
		dayHours[key].total++
		dayHours[key].high += record.HighConsumption
	}
	rates := forecastRates{hour: make(map[int]float64), dayHour: make(map[string]float64)}
	for key, value := range hours {
		rates.hour[key] = float64(value.high) / float64(value.total)
	}
	for key, value := range dayHours {
		rates.dayHour[key] = float64(value.high) / float64(value.total)
	}
	return rates
}
