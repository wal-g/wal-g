package internal

import (
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	"io"
)

type WalVerifyOutputType int

const (
	WalVerifyTableOutput WalVerifyOutputType = iota + 1
	WalVerifyJsonOutput
)

// WalVerifyOutputWriter writes the output of wal-verify command execution result
type WalVerifyOutputWriter interface {
	Write(result WalVerifyResult) error
}

// WalVerifyJsonOutputWriter writes the detailed JSON output
type WalVerifyJsonOutputWriter struct {
	output io.Writer
}

func (writer *WalVerifyJsonOutputWriter) Write(result WalVerifyResult) error {
	bytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = writer.output.Write(bytes)
	return err
}

// WalVerifyTableOutputWriter writes the output as pretty table
type WalVerifyTableOutputWriter struct {
	output io.Writer
}

func (writer *WalVerifyTableOutputWriter) Write(result WalVerifyResult) error {
	writer.writeTable(result)
	fmt.Println("WAL storage status: " + result.StorageStatus.String())
	return nil
}

func (writer *WalVerifyTableOutputWriter) writeTable(result WalVerifyResult) {
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(writer.output)
	defer tableWriter.Render()
	tableWriter.AppendHeader(table.Row{"TLI", "Start", "End", "Segments count", "Status"})

	for _, row := range result.IntegrityScanResult {
		tableWriter.AppendRow(table.Row{row.TimelineId, row.StartSegment, row.EndSegment, row.SegmentsCount, row.Status})
	}
}

func NewWalVerifyOutputWriter(outputType WalVerifyOutputType, output io.Writer) WalVerifyOutputWriter {
	switch outputType {
	case WalVerifyTableOutput:
		return &WalVerifyTableOutputWriter{output: output}
	case WalVerifyJsonOutput:
		return &WalVerifyJsonOutputWriter{output: output}
	default:
		return &WalVerifyJsonOutputWriter{output: output}
	}
}
