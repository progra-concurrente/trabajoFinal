package dataset

import (
	"testing"
	"time"
)

func TestProcessLineTransformsCleanRows(t *testing.T) {
	record, stats := processLine("16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000")
	if stats.RowsClean != 1 || record.Hour != 17 || record.HighConsumption != 1 {
		t.Fatalf("unexpected record=%+v stats=%+v", record, stats)
	}
	if record.Timestamp.IsZero() {
		t.Fatal("timestamp must be retained for forecast windows")
	}
}

func TestProcessLineRejectsMissingAndInvalidRows(t *testing.T) {
	for _, line := range []string{
		"16/12/2006;17:24:00;?;0.418;234.840;18.400;0.000;1.000;17.000",
		"16/12/2006;17:24:00;-1;0.418;234.840;18.400;0.000;1.000;17.000",
	} {
		_, stats := processLine(line)
		if stats.RowsClean != 0 {
			t.Fatalf("expected rejection for %q", line)
		}
	}
}

func TestForecastWindowPredictsFutureNotPresent(t *testing.T) {
	start := time.Date(2026, time.June, 25, 20, 0, 0, 0, time.UTC)
	records := make([]PowerConsumption, 50)
	for index := range records {
		active := .8
		if index >= 15 && index < 25 {
			active = 2
		}
		records[index] = PowerConsumption{Timestamp: start.Add(time.Duration(index) * time.Minute),
			GlobalActivePower: active, Voltage: 240, Hour: recordsTimeHour(start, index), DayOfWeek: 4, Month: 6}
		if active >= highConsumptionThreshold {
			records[index].HighConsumption = 1
		}
	}
	forecast := buildForecastRecords(records, 4)
	if len(forecast) == 0 {
		t.Fatal("expected forecast records")
	}
	first := forecast[0]
	if first.CurrentActivePower != .8 {
		t.Fatalf("current status should still be normal: %+v", first)
	}
	if first.FutureHighConsumption != 1 {
		t.Fatalf("future label should detect 10 high minutes: %+v", first)
	}
}

func TestForecastWindowRejectsTimeGaps(t *testing.T) {
	start := time.Now().Truncate(time.Minute)
	records := make([]PowerConsumption, 50)
	for index := range records {
		records[index] = PowerConsumption{Timestamp: start.Add(time.Duration(index) * time.Minute), Voltage: 240, Month: 6}
	}
	records[20].Timestamp = records[20].Timestamp.Add(time.Minute)
	if len(buildForecastRecords(records, 2)) != 0 {
		t.Fatal("windows crossing gaps must be excluded")
	}
}

func recordsTimeHour(start time.Time, index int) int {
	return start.Add(time.Duration(index) * time.Minute).Hour()
}
