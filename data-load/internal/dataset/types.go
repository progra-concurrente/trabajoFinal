package dataset

import "time"

const highConsumptionThreshold = 1.528

type ParsedPowerConsumption struct {
	Date                string
	Time                string
	Timestamp           time.Time
	GlobalActivePower   float64
	GlobalReactivePower float64
	Voltage             float64
	GlobalIntensity     float64
	SubMetering1        float64
	SubMetering2        float64
	SubMetering3        float64
	HasMissingValues    bool
}

type PowerConsumption struct {
	Timestamp           time.Time
	Date                string
	Time                string
	GlobalActivePower   float64
	GlobalReactivePower float64
	Voltage             float64
	GlobalIntensity     float64
	SubMetering1        float64
	SubMetering2        float64
	SubMetering3        float64
	SubMeteringTotal    float64
	Hour                int
	DayOfWeek           int
	DayOfMonth          int
	Month               int
	HighConsumption     int
	OtherConsumption    float64
}

type LogisticModel struct {
	ModelType         string                `json:"model_type"`
	Target            string                `json:"target"`
	Features          []string              `json:"features"`
	Weights           []float64             `json:"weights"`
	LearningRate      float64               `json:"learning_rate"`
	Epochs            int                   `json:"epochs"`
	Workers           int                   `json:"workers"`
	Rows              int                   `json:"rows"`
	TrainRows         int                   `json:"train_rows"`
	TestRows          int                   `json:"test_rows"`
	TrainRatio        float64               `json:"train_ratio"`
	SplitStrategy     string                `json:"split_strategy"`
	Threshold         float64               `json:"high_consumption_threshold"`
	DecisionThreshold float64               `json:"decision_threshold"`
	HistoryMinutes    int                   `json:"history_minutes"`
	HorizonMinutes    int                   `json:"horizon_minutes"`
	SustainedMinutes  int                   `json:"sustained_minutes"`
	Accuracy          float64               `json:"accuracy"`
	Loss              float64               `json:"loss"`
	TrainingMetrics   ClassificationMetrics `json:"training_metrics"`
	TestMetrics       ClassificationMetrics `json:"test_metrics"`
}

type ClassificationMetrics struct {
	Rows             int     `json:"rows"`
	TruePositive     int     `json:"true_positive"`
	TrueNegative     int     `json:"true_negative"`
	FalsePositive    int     `json:"false_positive"`
	FalseNegative    int     `json:"false_negative"`
	Accuracy         float64 `json:"accuracy"`
	Precision        float64 `json:"precision"`
	Recall           float64 `json:"recall"`
	Specificity      float64 `json:"specificity"`
	F1Score          float64 `json:"f1_score"`
	BalancedAccuracy float64 `json:"balanced_accuracy"`
	PositiveRate     float64 `json:"positive_rate"`
	Loss             float64 `json:"loss"`
}

type DemandAggregate struct {
	Group               string  `json:"group"`
	Records             int     `json:"records"`
	AverageActivePower  float64 `json:"average_active_power"`
	HighConsumption     int     `json:"high_consumption"`
	HighConsumptionRate float64 `json:"high_consumption_rate"`
	AverageOther        float64 `json:"average_other_consumption"`
}

type CombinedDemandAggregate struct {
	PrimaryGroup        string  `json:"primary_group"`
	SecondaryGroup      string  `json:"secondary_group"`
	Records             int     `json:"records"`
	AverageActivePower  float64 `json:"average_active_power"`
	HighConsumption     int     `json:"high_consumption"`
	HighConsumptionRate float64 `json:"high_consumption_rate"`
	AverageOther        float64 `json:"average_other_consumption"`
}

type AnalysisCriteria struct {
	MinimumDayHourRecords      int `json:"minimum_day_hour_records"`
	MinimumCalendarDateRecords int `json:"minimum_calendar_date_records"`
}

type ReportDemandPeak struct {
	Group               string  `json:"group"`
	Records             int     `json:"records"`
	AverageActivePower  float64 `json:"average_active_power"`
	HighConsumptionRate float64 `json:"high_consumption_rate"`
}

type ReportCombinedPeak struct {
	PrimaryGroup        string  `json:"primary_group"`
	SecondaryGroup      string  `json:"secondary_group"`
	Records             int     `json:"records"`
	AverageActivePower  float64 `json:"average_active_power"`
	HighConsumptionRate float64 `json:"high_consumption_rate"`
}

type SustainabilityReport struct {
	BusinessMission   string                    `json:"business_mission"`
	Objective         string                    `json:"objective"`
	Target            string                    `json:"target"`
	Threshold         float64                   `json:"threshold"`
	AnalysisCriteria  AnalysisCriteria          `json:"analysis_criteria"`
	PeakHours         []ReportDemandPeak        `json:"peak_hours"`
	PeakDayHours      []ReportCombinedPeak      `json:"peak_day_hours"`
	PeakCalendarDates []ReportDemandPeak        `json:"peak_calendar_dates"`
	HourlyPatterns    []DemandAggregate         `json:"hourly_patterns"`
	DailyPatterns     []DemandAggregate         `json:"daily_patterns"`
	MonthlyPatterns   []DemandAggregate         `json:"monthly_patterns"`
	DayHourPatterns   []CombinedDemandAggregate `json:"day_hour_patterns"`
	Recommendations   []string                  `json:"recommendations"`
}

type LoadStats struct {
	LinesRead      int
	RowsClean      int
	DroppedMissing int
	DroppedInvalid int
}

func (stats *LoadStats) Add(other LoadStats) {
	stats.LinesRead += other.LinesRead
	stats.RowsClean += other.RowsClean
	stats.DroppedMissing += other.DroppedMissing
	stats.DroppedInvalid += other.DroppedInvalid
}
