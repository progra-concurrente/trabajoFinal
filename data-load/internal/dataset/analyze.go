package dataset

import (
	"fmt"
	"sort"
)

const (
	minimumDayHourRecords      = 1000
	minimumCalendarDateRecords = 5000
)

type chunkResult struct {
	Index   int
	Records []PowerConsumption
	Stats   LoadStats
}

type aggregateAccumulator struct {
	group            string
	records          int
	activePower      float64
	highConsumption  int
	otherConsumption float64
}

type demandAnalysis struct {
	Hourly       []DemandAggregate
	Daily        []DemandAggregate
	Monthly      []DemandAggregate
	DayHour      []CombinedDemandAggregate
	CalendarDate []DemandAggregate
	MonthHour    []CombinedDemandAggregate
}

func analyzeLine(stats *LoadStats, parsed ParsedPowerConsumption, parseErr error, valid bool) {
	stats.LinesRead++

	if parseErr != nil {
		stats.DroppedInvalid++
		return
	}

	if parsed.HasMissingValues {
		stats.DroppedMissing++
		return
	}

	if !valid {
		stats.DroppedInvalid++
		return
	}

	stats.RowsClean++
}

func buildSustainabilityReport(records []PowerConsumption) SustainabilityReport {
	return buildSustainabilityReportFromAnalysis(analyzeDemand(records))
}

func analyzeDemand(records []PowerConsumption) demandAnalysis {
	return demandAnalysis{
		Hourly: aggregateBy(records, func(record PowerConsumption) string {
			return fmt.Sprintf("%02d:00", record.Hour)
		}),
		Daily: aggregateBy(records, func(record PowerConsumption) string {
			return dayName(record.DayOfWeek)
		}),
		Monthly: aggregateBy(records, func(record PowerConsumption) string {
			return monthName(record.Month)
		}),
		DayHour: aggregateByCombination(records,
			func(record PowerConsumption) string { return dayName(record.DayOfWeek) },
			func(record PowerConsumption) string { return fmt.Sprintf("%02d:00", record.Hour) },
		),
		CalendarDate: aggregateBy(records, func(record PowerConsumption) string {
			return fmt.Sprintf("%s %02d", monthName(record.Month), record.DayOfMonth)
		}),
		MonthHour: aggregateByCombination(records,
			func(record PowerConsumption) string { return monthName(record.Month) },
			func(record PowerConsumption) string { return fmt.Sprintf("%02d:00", record.Hour) },
		),
	}
}

func buildSustainabilityReportFromAnalysis(analysis demandAnalysis) SustainabilityReport {
	return SustainabilityReport{
		BusinessMission: "Promover eficiencia energetica domestica detectando patrones de alto consumo electrico.",
		Objective:       "Identificar cuando ocurre el alto consumo para orientar alertas y recomendaciones de ahorro.",
		Target:          "HighConsumption",
		Threshold:       highConsumptionThreshold,
		AnalysisCriteria: AnalysisCriteria{
			MinimumDayHourRecords:      minimumDayHourRecords,
			MinimumCalendarDateRecords: minimumCalendarDateRecords,
		},
		PeakHours: reportDemandPeaks(
			topAggregates(analysis.Hourly, 3),
		),
		PeakDayHours: reportCombinedPeaks(
			topCombinedAggregates(analysis.DayHour, minimumDayHourRecords, 5),
		),
		PeakCalendarDates: reportDemandPeaks(
			topAggregatesWithMinimum(analysis.CalendarDate, minimumCalendarDateRecords, 5),
		),
		Recommendations: []string{
			"Priorizar alertas en las combinaciones de dia y hora con mayor tasa de alto consumo.",
			"Reforzar recomendaciones en las fechas de calendario que repiten picos entre anos.",
			"Usar la deteccion de HighConsumption como alerta para promover eficiencia energetica.",
		},
	}
}

func reportDemandPeaks(aggregates []DemandAggregate) []ReportDemandPeak {
	peaks := make([]ReportDemandPeak, 0, len(aggregates))
	for _, aggregate := range aggregates {
		peaks = append(peaks, ReportDemandPeak{
			Group:               aggregate.Group,
			Records:             aggregate.Records,
			AverageActivePower:  aggregate.AverageActivePower,
			HighConsumptionRate: aggregate.HighConsumptionRate,
		})
	}
	return peaks
}

func reportCombinedPeaks(aggregates []CombinedDemandAggregate) []ReportCombinedPeak {
	peaks := make([]ReportCombinedPeak, 0, len(aggregates))
	for _, aggregate := range aggregates {
		peaks = append(peaks, ReportCombinedPeak{
			PrimaryGroup:        aggregate.PrimaryGroup,
			SecondaryGroup:      aggregate.SecondaryGroup,
			Records:             aggregate.Records,
			AverageActivePower:  aggregate.AverageActivePower,
			HighConsumptionRate: aggregate.HighConsumptionRate,
		})
	}
	return peaks
}

func aggregateByCombination(
	records []PowerConsumption,
	primaryFn func(PowerConsumption) string,
	secondaryFn func(PowerConsumption) string,
) []CombinedDemandAggregate {
	accumulators := make(map[string]*aggregateAccumulator)
	primaryGroups := make(map[string]string)
	secondaryGroups := make(map[string]string)

	for _, record := range records {
		primary := primaryFn(record)
		secondary := secondaryFn(record)
		key := primary + "\x00" + secondary
		accumulator, ok := accumulators[key]
		if !ok {
			accumulator = &aggregateAccumulator{}
			accumulators[key] = accumulator
			primaryGroups[key] = primary
			secondaryGroups[key] = secondary
		}

		accumulator.records++
		accumulator.activePower += record.GlobalActivePower
		accumulator.highConsumption += record.HighConsumption
		accumulator.otherConsumption += record.OtherConsumption
	}

	aggregates := make([]CombinedDemandAggregate, 0, len(accumulators))
	for key, accumulator := range accumulators {
		if accumulator.records == 0 {
			continue
		}
		recordsCount := float64(accumulator.records)
		aggregates = append(aggregates, CombinedDemandAggregate{
			PrimaryGroup:        primaryGroups[key],
			SecondaryGroup:      secondaryGroups[key],
			Records:             accumulator.records,
			AverageActivePower:  accumulator.activePower / recordsCount,
			HighConsumption:     accumulator.highConsumption,
			HighConsumptionRate: float64(accumulator.highConsumption) / recordsCount,
			AverageOther:        accumulator.otherConsumption / recordsCount,
		})
	}

	sort.Slice(aggregates, func(i, j int) bool {
		if aggregates[i].PrimaryGroup == aggregates[j].PrimaryGroup {
			return aggregates[i].SecondaryGroup < aggregates[j].SecondaryGroup
		}
		return aggregates[i].PrimaryGroup < aggregates[j].PrimaryGroup
	})
	return aggregates
}

func topCombinedAggregates(
	aggregates []CombinedDemandAggregate,
	minimumRecords int,
	limit int,
) []CombinedDemandAggregate {
	ranked := make([]CombinedDemandAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.Records >= minimumRecords {
			ranked = append(ranked, aggregate)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].HighConsumptionRate == ranked[j].HighConsumptionRate {
			return ranked[i].AverageActivePower > ranked[j].AverageActivePower
		}
		return ranked[i].HighConsumptionRate > ranked[j].HighConsumptionRate
	})
	if limit < len(ranked) {
		ranked = ranked[:limit]
	}
	return ranked
}

func topAggregatesWithMinimum(
	aggregates []DemandAggregate,
	minimumRecords int,
	limit int,
) []DemandAggregate {
	filtered := make([]DemandAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.Records >= minimumRecords {
			filtered = append(filtered, aggregate)
		}
	}
	return topAggregates(filtered, limit)
}

func aggregateBy(records []PowerConsumption, groupFn func(PowerConsumption) string) []DemandAggregate {
	accumulators := make(map[string]*aggregateAccumulator)
	for _, record := range records {
		group := groupFn(record)
		accumulator, ok := accumulators[group]
		if !ok {
			accumulator = &aggregateAccumulator{group: group}
			accumulators[group] = accumulator
		}

		accumulator.records++
		accumulator.activePower += record.GlobalActivePower
		accumulator.highConsumption += record.HighConsumption
		accumulator.otherConsumption += record.OtherConsumption
	}

	aggregates := make([]DemandAggregate, 0, len(accumulators))
	for _, accumulator := range accumulators {
		if accumulator.records == 0 {
			continue
		}
		recordsCount := float64(accumulator.records)
		aggregates = append(aggregates, DemandAggregate{
			Group:               accumulator.group,
			Records:             accumulator.records,
			AverageActivePower:  accumulator.activePower / recordsCount,
			HighConsumption:     accumulator.highConsumption,
			HighConsumptionRate: float64(accumulator.highConsumption) / recordsCount,
			AverageOther:        accumulator.otherConsumption / recordsCount,
		})
	}

	sort.Slice(aggregates, func(i, j int) bool {
		return aggregates[i].Group < aggregates[j].Group
	})
	return aggregates
}

func topAggregates(aggregates []DemandAggregate, limit int) []DemandAggregate {
	ranked := append([]DemandAggregate(nil), aggregates...)
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].HighConsumptionRate == ranked[j].HighConsumptionRate {
			return ranked[i].AverageActivePower > ranked[j].AverageActivePower
		}
		return ranked[i].HighConsumptionRate > ranked[j].HighConsumptionRate
	})

	if limit > len(ranked) {
		limit = len(ranked)
	}
	return ranked[:limit]
}

func dayName(day int) string {
	names := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	if day < 0 || day >= len(names) {
		return "Unknown"
	}
	return names[day]
}

func monthName(month int) string {
	names := []string{
		"Unknown",
		"January",
		"February",
		"March",
		"April",
		"May",
		"June",
		"July",
		"August",
		"September",
		"October",
		"November",
		"December",
	}
	if month < 1 || month >= len(names) {
		return "Unknown"
	}
	return names[month]
}
