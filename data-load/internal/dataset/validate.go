package dataset

func validateRecord(record ParsedPowerConsumption) bool {
	if record.HasMissingValues {
		return false
	}

	if record.Timestamp.IsZero() {
		return false
	}

	return record.GlobalActivePower >= 0 &&
		record.GlobalReactivePower >= 0 &&
		record.Voltage > 0 &&
		record.GlobalIntensity >= 0 &&
		record.SubMetering1 >= 0 &&
		record.SubMetering2 >= 0 &&
		record.SubMetering3 >= 0
}
