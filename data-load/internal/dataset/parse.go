package dataset

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func parseLine(line string) (ParsedPowerConsumption, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 9 {
		return ParsedPowerConsumption{}, fmt.Errorf("expected 9 fields, got %d", len(parts))
	}

	timestamp, err := time.Parse("2/1/2006 15:04:05", parts[0]+" "+parts[1])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}

	globalActivePower, missingGlobalActivePower, err := parseFloat(parts[2])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}
	globalReactivePower, missingGlobalReactivePower, err := parseFloat(parts[3])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}
	voltage, missingVoltage, err := parseFloat(parts[4])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}
	globalIntensity, missingGlobalIntensity, err := parseFloat(parts[5])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}
	subMetering1, missingSubMetering1, err := parseFloat(parts[6])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}
	subMetering2, missingSubMetering2, err := parseFloat(parts[7])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}
	subMetering3, missingSubMetering3, err := parseFloat(parts[8])
	if err != nil {
		return ParsedPowerConsumption{}, err
	}

	return ParsedPowerConsumption{
		Date:                parts[0],
		Time:                parts[1],
		Timestamp:           timestamp,
		GlobalActivePower:   globalActivePower,
		GlobalReactivePower: globalReactivePower,
		Voltage:             voltage,
		GlobalIntensity:     globalIntensity,
		SubMetering1:        subMetering1,
		SubMetering2:        subMetering2,
		SubMetering3:        subMetering3,
		HasMissingValues: missingGlobalActivePower ||
			missingGlobalReactivePower ||
			missingVoltage ||
			missingGlobalIntensity ||
			missingSubMetering1 ||
			missingSubMetering2 ||
			missingSubMetering3,
	}, nil
}

func parseFloat(value string) (float64, bool, error) {
	cleanValue := strings.TrimSpace(strings.ReplaceAll(value, ",", "."))
	if cleanValue == "" || cleanValue == "?" {
		return 0, true, nil
	}

	parsed, err := strconv.ParseFloat(cleanValue, 64)
	if err != nil {
		return 0, false, err
	}

	return parsed, false, nil
}
