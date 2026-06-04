package dataset

import (
	"fmt"
	"sort"
)

type chunkResult struct {
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
	hourly := aggregateBy(records, func(record PowerConsumption) string {
		return fmt.Sprintf("%02d:00", record.Hour)
	})
	daily := aggregateBy(records, func(record PowerConsumption) string {
		return dayName(record.DayOfWeek)
	})
	monthly := aggregateBy(records, func(record PowerConsumption) string {
		return monthName(record.Month)
	})

	return SustainabilityReport{
		BusinessMission: "Promover eficiencia energetica domestica detectando patrones de alto consumo electrico.",
		Objective:       "Identificar horarios, dias y meses con mayor demanda para apoyar recomendaciones de ahorro y desplazamiento de consumo.",
		Target:          "HighConsumption",
		Threshold:       highConsumptionThreshold,
		PeakHours:       topAggregates(hourly, 5),
		PeakDays:        topAggregates(daily, 3),
		PeakMonths:      topAggregates(monthly, 3),
		Recommendations: []string{
			"Priorizar recomendaciones de ahorro en las horas con mayor tasa de alto consumo.",
			"Monitorear la demanda domestica nocturna y matutina para reducir picos evitables.",
			"Usar la deteccion de HighConsumption como alerta para promover eficiencia energetica.",
		},
	}
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
