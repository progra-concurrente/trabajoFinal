package dataset

import "testing"

func TestProcessLineTransformsCleanRows(t *testing.T) {
	line := "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000"

	record, stats := processLine(line)

	if stats.LinesRead != 1 || stats.RowsClean != 1 {
		t.Fatalf("expected one clean row, got stats: %+v", stats)
	}
	if record.Hour != 17 {
		t.Fatalf("expected hour 17, got %d", record.Hour)
	}
	if record.DayOfWeek != 6 {
		t.Fatalf("expected Saturday as day 6, got %d", record.DayOfWeek)
	}
	if record.Month != 12 {
		t.Fatalf("expected month 12, got %d", record.Month)
	}
	if record.HighConsumption != 1 {
		t.Fatalf("expected high consumption label 1, got %d", record.HighConsumption)
	}

	expectedSubMeteringTotal := 0.0 + 1.0 + 17.0
	if record.SubMeteringTotal != expectedSubMeteringTotal {
		t.Fatalf("expected sub metering total %.12f, got %.12f", expectedSubMeteringTotal, record.SubMeteringTotal)
	}

	expectedOtherConsumption := 4.216*1000/60 - expectedSubMeteringTotal
	if record.OtherConsumption != expectedOtherConsumption {
		t.Fatalf("expected other consumption %.12f, got %.12f", expectedOtherConsumption, record.OtherConsumption)
	}
}

func TestProcessLineDropsMissingRows(t *testing.T) {
	line := "16/12/2006;17:24:00;?;0.418;234.840;18.400;0.000;1.000;17.000"

	record, stats := processLine(line)

	if stats.LinesRead != 1 || stats.RowsClean != 0 || stats.DroppedMissing != 1 {
		t.Fatalf("expected missing row to be dropped, got record %+v and stats %+v", record, stats)
	}
}

func TestProcessLineDropsInvalidRows(t *testing.T) {
	cases := []string{
		"16/12/2006;17:24:00;-1.000;0.418;234.840;18.400;0.000;1.000;17.000",
		"16/12/2006;17:24:00;4.216;0.418;0.000;18.400;0.000;1.000;17.000",
		"16/12/2006;17:24:00;4.216;0.418;234.840;-1.000;0.000;1.000;17.000",
		"16/12/2006;17:24:00;4.216;0.418;234.840;18.400;-1.000;1.000;17.000",
		"not-date;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000",
		"16/12/2006;17:24:00;4.216",
	}

	for _, line := range cases {
		_, stats := processLine(line)
		if stats.RowsClean != 0 || stats.DroppedInvalid != 1 {
			t.Fatalf("expected invalid row to be dropped for %q, got stats %+v", line, stats)
		}
	}
}

func TestTrainLogisticModelUsesParallelWorkers(t *testing.T) {
	records := []PowerConsumption{
		{GlobalReactivePower: 0.1, Voltage: 240, GlobalIntensity: 2, SubMeteringTotal: 1, OtherConsumption: 1, Hour: 1, DayOfWeek: 1, Month: 1, HighConsumption: 0},
		{GlobalReactivePower: 0.4, Voltage: 235, GlobalIntensity: 18, SubMeteringTotal: 18, OtherConsumption: 52, Hour: 17, DayOfWeek: 6, Month: 12, HighConsumption: 1},
		{GlobalReactivePower: 0.2, Voltage: 241, GlobalIntensity: 3, SubMeteringTotal: 2, OtherConsumption: 2, Hour: 2, DayOfWeek: 2, Month: 2, HighConsumption: 0},
		{GlobalReactivePower: 0.5, Voltage: 233, GlobalIntensity: 20, SubMeteringTotal: 19, OtherConsumption: 55, Hour: 19, DayOfWeek: 5, Month: 11, HighConsumption: 1},
	}

	model := trainLogisticModel(records, 2)

	if model.ModelType != "logistic_regression" {
		t.Fatalf("expected logistic regression model, got %s", model.ModelType)
	}
	if model.Workers != 2 {
		t.Fatalf("expected 2 workers, got %d", model.Workers)
	}
	if model.Rows != len(records) {
		t.Fatalf("expected %d rows, got %d", len(records), model.Rows)
	}
	if len(model.Weights) != len(logisticFeatureNames)+1 {
		t.Fatalf("expected weights for bias and features, got %d", len(model.Weights))
	}
}

func TestBuildSustainabilityReportFindsPeakHours(t *testing.T) {
	records := []PowerConsumption{
		{GlobalActivePower: 0.7, Hour: 6, DayOfWeek: 1, Month: 1, HighConsumption: 0, OtherConsumption: 4},
		{GlobalActivePower: 1.8, Hour: 18, DayOfWeek: 1, Month: 1, HighConsumption: 1, OtherConsumption: 20},
		{GlobalActivePower: 2.1, Hour: 18, DayOfWeek: 1, Month: 1, HighConsumption: 1, OtherConsumption: 24},
		{GlobalActivePower: 1.9, Hour: 19, DayOfWeek: 2, Month: 2, HighConsumption: 1, OtherConsumption: 22},
	}

	report := buildSustainabilityReport(records)

	if report.BusinessMission == "" {
		t.Fatal("expected business mission in sustainability report")
	}
	if len(report.PeakHours) == 0 {
		t.Fatal("expected peak hours in sustainability report")
	}
	if report.PeakHours[0].Group != "18:00" {
		t.Fatalf("expected 18:00 as top peak hour, got %s", report.PeakHours[0].Group)
	}
	if report.PeakHours[0].HighConsumptionRate != 1 {
		t.Fatalf("expected high consumption rate 1, got %.6f", report.PeakHours[0].HighConsumptionRate)
	}
}
