package dataset

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	sharedml "powersight/pkg/ml"
)

func saveToCSV(filePath string, data []PowerConsumption) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString("Date,Time,GlobalActivePower,GlobalReactivePower,Voltage,GlobalIntensity,SubMetering1,SubMetering2,SubMetering3,SubMeteringTotal,Hour,DayOfWeek,DayOfMonth,Month,HighConsumption,OtherConsumption\n")
	if err != nil {
		return err
	}
	for _, record := range data {
		line := fmt.Sprintf("%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%.3f\n",
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
			record.DayOfMonth,
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

func saveForecastCSV(filePath string, data []sharedml.Record) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	_, err = writer.WriteString("Index,CurrentActivePower,RecentAverageActivePower,RecentMaximumActivePower,RecentStdDevActivePower,RecentActivePowerTrend,CurrentReactivePower,CurrentVoltage,CurrentIntensity,CurrentSubMeteringTotal,CurrentOtherConsumption,Hour,DayOfWeek,Month,HistoricalHourHighRate,HistoricalDayHourHighRate,FutureHighConsumption\n")
	if err != nil {
		return err
	}
	for index, record := range data {
		f := record.ForecastFeatures
		line := fmt.Sprintf("%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d,%d,%d,%.6f,%.6f,%d\n",
			index, f.CurrentActivePower, f.RecentAverageActivePower, f.RecentMaximumActivePower,
			f.RecentStdDevActivePower, f.RecentActivePowerTrend, f.CurrentReactivePower,
			f.CurrentVoltage, f.CurrentIntensity, f.CurrentSubMeteringTotal, f.CurrentOtherConsumption,
			f.Hour, f.DayOfWeek, f.Month, f.HistoricalHourHighRate, f.HistoricalDayHourHighRate,
			record.FutureHighConsumption)
		if _, err := writer.WriteString(line); err != nil {
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

func saveCombinedAggregatesCSV(filePath string, aggregates []CombinedDemandAggregate) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString("PrimaryGroup,SecondaryGroup,Records,AverageActivePower,HighConsumption,HighConsumptionRate,AverageOtherConsumption\n")
	if err != nil {
		return err
	}

	for _, aggregate := range aggregates {
		line := fmt.Sprintf("%s,%s,%d,%.6f,%d,%.6f,%.6f\n",
			aggregate.PrimaryGroup,
			aggregate.SecondaryGroup,
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
