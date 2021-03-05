package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// CSVRecord encapsulates a single line's data.
type CSVRecord struct {
	lineNo  int
	columns []string
}

// ReadCSVFile reads the CSV and writes to the channel. Returns an error if it
// cannot open the file to read, or if there is an empty line, or if every line
// does not contain the same number of columns.
func ReadCSVFile(path string, outputChan chan<- CSVRecord) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	csvreader := csv.NewReader(f)

	numRecords := 0
	numColumns := -1
	for {
		numRecords++
		record, err := csvreader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		recordLen := len(record)
		if recordLen == 0 {
			return fmt.Errorf("empty record found on line %d", numRecords)
		}

		// If this is the first line, then set the expected columns. All lines
		// should have the same number of columns.
		if numColumns == -1 {
			numColumns = recordLen
		} else if numColumns != recordLen {
			return fmt.Errorf(
				"expected %d columns but found %d columns on line %d",
				numColumns,
				recordLen,
				numRecords)
		}
		outputChan <- CSVRecord{numRecords, record}
	}
	return nil
}
