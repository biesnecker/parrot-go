package main

import (
	"encoding/csv"
	"os"
)

// WriteCSV receives records from the inputChan and writes them to the output
// path using the encoding/csv module.
func WriteCSV(path string, inputChan <-chan []string) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	csvwriter := csv.NewWriter(f)
	for record := range inputChan {
		if err := csvwriter.Write(record); err != nil {
			return err
		}
	}

	csvwriter.Flush()
	return nil
}
