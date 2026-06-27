package api

import (
	"testing"
	"time"

	"powersight/internal/store"
)

func TestRecommendationPreventsFuturePeak(t *testing.T) {
	current := store.CurrentStatus{ActivePowerKW: 1.2, ThresholdKW: highThresholdKW, CurrentlyHigh: false}
	recommendation := recommendationFor(current, "alto", .08, store.TimeContext{IsPeak: true})
	if recommendation.Title == "" || len(recommendation.Actions) < 2 {
		t.Fatalf("not actionable: %+v", recommendation)
	}
}

func TestTemporalContextUsesHistoricalRates(t *testing.T) {
	value := time.Date(2026, time.June, 25, 21, 0, 0, 0, time.FixedZone("Lima", -5*3600))
	context := temporalContext(value, .56, .63)
	if !context.IsPeak || context.HourHighRate != .56 || context.DayHourHighRate != .63 {
		t.Fatalf("unexpected context: %+v", context)
	}
}

func TestBuildForecastFeaturesUsesFifteenMinuteTrend(t *testing.T) {
	location := time.FixedZone("Lima", -5*3600)
	server := &Server{location: location, historical: historicalRates{
		Hour: map[int]float64{21: .56}, DayHour: map[string]float64{"4-21": .63},
	}}
	start := time.Date(2026, time.June, 25, 20, 46, 0, 0, location)
	request := forecastRequest{Readings: make([]readingRequest, requiredReadings)}
	for index := range request.Readings {
		request.Readings[index] = readingRequest{ObservedAt: start.Add(time.Duration(index) * time.Minute),
			GlobalActivePower: .8 + .04*float64(index), GlobalReactivePower: .1, Voltage: 240,
			GlobalIntensity: 4, SubMetering2: 1, SubMetering3: 2}
	}
	features, latest, current, context, err := server.buildForecastFeatures(request)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Hour() != 21 || features.RecentActivePowerTrend <= 0 || current.CurrentlyHigh {
		t.Fatalf("unexpected forecast input: %+v %+v", features, current)
	}
	if context.DayHourHighRate != .63 {
		t.Fatalf("historical context missing: %+v", context)
	}
}
