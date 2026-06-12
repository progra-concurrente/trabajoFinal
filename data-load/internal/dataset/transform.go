package dataset

func transformRecord(record ParsedPowerConsumption) PowerConsumption {
	highConsumption := 0
	if record.GlobalActivePower >= highConsumptionThreshold {
		highConsumption = 1
	}

	activeEnergyPerMinute := record.GlobalActivePower * 1000 / 60
	subMeteringTotal := record.SubMetering1 + record.SubMetering2 + record.SubMetering3
	otherConsumption := activeEnergyPerMinute - subMeteringTotal

	return PowerConsumption{
		Date:                record.Date,
		Time:                record.Time,
		GlobalActivePower:   record.GlobalActivePower,
		GlobalReactivePower: record.GlobalReactivePower,
		Voltage:             record.Voltage,
		GlobalIntensity:     record.GlobalIntensity,
		SubMetering1:        record.SubMetering1,
		SubMetering2:        record.SubMetering2,
		SubMetering3:        record.SubMetering3,
		SubMeteringTotal:    subMeteringTotal,
		Hour:                record.Timestamp.Hour(),
		DayOfWeek:           int(record.Timestamp.Weekday()),
		DayOfMonth:          record.Timestamp.Day(),
		Month:               int(record.Timestamp.Month()),
		HighConsumption:     highConsumption,
		OtherConsumption:    otherConsumption,
	}
}
