package dataset

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func saveToCSV(filePath string, data []PowerConsumption) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString("Date,Time,GlobalActivePower,GlobalReactivePower,Voltage,GlobalIntensity,SubMetering1,SubMetering2,SubMetering3,SubMeteringTotal,Hour,DayOfWeek,Month,HighConsumption,OtherConsumption\n")
	if err != nil {
		return err
	}
	for _, record := range data {
		line := fmt.Sprintf("%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%.3f\n",
			record.Date,
			record.Time,
			record.GlobalActivePower,
			record.GlobalReactivePower,
			record.Voltage,
			record.GlobalIntensity,
			record.SubMetering1,
			record.SubMetering2,
			record.SubMetering3,
			record.SubMeteringTotal,
			record.Hour,
			record.DayOfWeek,
			record.Month,
			record.HighConsumption,
			record.OtherConsumption,
		)
		_, err := writer.WriteString(line)
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}

func saveModelJSON(filePath string, model LogisticModel) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(model)
}

func saveSustainabilityReportJSON(filePath string, report SustainabilityReport) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func saveAggregatesCSV(filePath string, aggregates []DemandAggregate) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString("Group,Records,AverageActivePower,HighConsumption,HighConsumptionRate,AverageOtherConsumption\n")
	if err != nil {
		return err
	}

	for _, aggregate := range aggregates {
		line := fmt.Sprintf("%s,%d,%.6f,%d,%.6f,%.6f\n",
			aggregate.Group,
			aggregate.Records,
			aggregate.AverageActivePower,
			aggregate.HighConsumption,
			aggregate.HighConsumptionRate,
			aggregate.AverageOther,
		)
		if _, err := writer.WriteString(line); err != nil {
			return err
		}
	}

	return writer.Flush()
}
