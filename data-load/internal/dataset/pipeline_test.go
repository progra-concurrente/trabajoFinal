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
	if record.DayOfMonth != 16 {
		t.Fatalf("expected day of month 16, got %d", record.DayOfMonth)
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

func TestProcessLineAcceptsDatesFromOtherYears(t *testing.T) {
	line := "6/9/2007;14:43:00;0.234;0.162;240.760;1.200;0.000;1.000;0.000"

	record, stats := processLine(line)

	if stats.RowsClean != 1 || stats.DroppedInvalid != 0 {
		t.Fatalf("expected a clean row with single-digit date fields, got record %+v and stats %+v", record, stats)
	}
	if record.Month != 9 || record.Hour != 14 {
		t.Fatalf("expected September at 14:00, got month %d and hour %d", record.Month, record.Hour)
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

	trainingRecords, testRecords := splitTrainTest(records, 0.75)
	model := trainLogisticModel(trainingRecords, testRecords, 2)

	if model.ModelType != "logistic_regression" {
		t.Fatalf("expected logistic regression model, got %s", model.ModelType)
	}
	if model.Workers != 2 {
		t.Fatalf("expected 2 workers, got %d", model.Workers)
	}
	if model.Rows != len(records) {
		t.Fatalf("expected %d rows, got %d", len(records), model.Rows)
	}
	if model.TrainRows != 3 || model.TestRows != 1 {
		t.Fatalf("expected a 3/1 train-test split, got %d/%d", model.TrainRows, model.TestRows)
	}
	if len(model.Weights) != len(logisticFeatureNames)+1 {
		t.Fatalf("expected weights for bias and features, got %d", len(model.Weights))
	}
	if model.TestMetrics.Rows != 1 {
		t.Fatalf("expected test metrics for one row, got %+v", model.TestMetrics)
	}
}

func TestSplitTrainTestPreservesTemporalOrder(t *testing.T) {
	records := []PowerConsumption{
		{Hour: 1},
		{Hour: 2},
		{Hour: 3},
		{Hour: 4},
		{Hour: 5},
	}

	trainingRecords, testRecords := splitTrainTest(records, 0.8)

	if len(trainingRecords) != 4 || len(testRecords) != 1 {
		t.Fatalf("expected 4/1 split, got %d/%d", len(trainingRecords), len(testRecords))
	}
	if trainingRecords[0].Hour != 1 || trainingRecords[3].Hour != 4 || testRecords[0].Hour != 5 {
		t.Fatalf("expected temporal order to be preserved, got train %+v test %+v", trainingRecords, testRecords)
	}
}

func TestClassificationMetricsUseConfusionMatrix(t *testing.T) {
	records := []PowerConsumption{
		{HighConsumption: 1},
		{HighConsumption: 0},
	}
	weights := make([]float64, len(logisticFeatureNames)+1)

	metrics := computeClassificationMetricsParallel(records, weights, 2)

	if metrics.TruePositive != 1 || metrics.FalsePositive != 1 {
		t.Fatalf("expected zero weights to predict both rows as positive, got %+v", metrics)
	}
	if metrics.Accuracy != 0.5 || metrics.Recall != 1 || metrics.Precision != 0.5 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestBuildSustainabilityReportFindsPeakHours(t *testing.T) {
	records := []PowerConsumption{
		{GlobalActivePower: 0.7, Hour: 6, DayOfWeek: 1, Month: 1, HighConsumption: 0, OtherConsumption: 4},
	}
	for i := 0; i < minimumCalendarDateRecords; i++ {
		records = append(records,
			PowerConsumption{GlobalActivePower: 2.1, Hour: 18, DayOfWeek: 1, DayOfMonth: 1, Month: 1, HighConsumption: 1, OtherConsumption: 20},
			PowerConsumption{GlobalActivePower: 1.9, Hour: 19, DayOfWeek: 2, DayOfMonth: 14, Month: 2, HighConsumption: 1, OtherConsumption: 22},
		)
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
	if len(report.PeakDayHours) == 0 {
		t.Fatal("expected ranked day-hour combinations")
	}
	if report.PeakDayHours[0].Records < minimumDayHourRecords {
		t.Fatalf("expected minimum day-hour records, got %+v", report.PeakDayHours[0])
	}
	if len(report.PeakCalendarDates) == 0 || report.PeakCalendarDates[0].Group != "January 01" {
		t.Fatalf("expected January 01 as a peak calendar date, got %+v", report.PeakCalendarDates)
	}
}
