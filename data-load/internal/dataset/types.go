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
	Month               int
	HighConsumption     int
	OtherConsumption    float64
}

type LogisticModel struct {
	ModelType    string    `json:"model_type"`
	Target       string    `json:"target"`
	Features     []string  `json:"features"`
	Weights      []float64 `json:"weights"`
	LearningRate float64   `json:"learning_rate"`
	Epochs       int       `json:"epochs"`
	Workers      int       `json:"workers"`
	Rows         int       `json:"rows"`
	Threshold    float64   `json:"threshold"`
	Accuracy     float64   `json:"accuracy"`
	Loss         float64   `json:"loss"`
}

type DemandAggregate struct {
	Group               string  `json:"group"`
	Records             int     `json:"records"`
	AverageActivePower  float64 `json:"average_active_power"`
	HighConsumption     int     `json:"high_consumption"`
	HighConsumptionRate float64 `json:"high_consumption_rate"`
	AverageOther        float64 `json:"average_other_consumption"`
}

type SustainabilityReport struct {
	BusinessMission string            `json:"business_mission"`
	Objective       string            `json:"objective"`
	Target          string            `json:"target"`
	Threshold       float64           `json:"threshold"`
	PeakHours       []DemandAggregate `json:"peak_hours"`
	PeakDays        []DemandAggregate `json:"peak_days"`
	PeakMonths      []DemandAggregate `json:"peak_months"`
	Recommendations []string          `json:"recommendations"`
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
