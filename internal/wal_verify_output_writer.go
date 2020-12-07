package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jedib0t/go-pretty/table"
)

type WalVerifyOutputType int

const (
	WalVerifyTableOutput WalVerifyOutputType = iota + 1
	WalVerifyJsonOutput
)

// WalVerifyOutputWriter writes the output of wal-verify command execution result
type WalVerifyOutputWriter interface {
	Write(results map[WalVerifyCheckType]WalVerifyCheckResult) error
}

// WalVerifyJsonOutputWriter writes the detailed JSON output
type WalVerifyJsonOutputWriter struct {
	output io.Writer
}

func (writer *WalVerifyJsonOutputWriter) Write(results map[WalVerifyCheckType]WalVerifyCheckResult) error {
	bytes, err := json.Marshal(results)
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

func (writer *WalVerifyTableOutputWriter) Write(result map[WalVerifyCheckType]WalVerifyCheckResult) error {
	for checkType, checkResult := range result {
		outputReader, err := newPrettyOutputReader(checkType, checkResult)
		if err != nil {
			return err
		}
		_, err = io.Copy(writer.output, outputReader)
		if err != nil {
			return err
		}
	}
	return nil
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

func newPrettyOutputReader(checkType WalVerifyCheckType, checkResult WalVerifyCheckResult) (io.Reader, error) {
	var outputBuffer bytes.Buffer
	outputBuffer.WriteString(fmt.Sprintf("[wal-verify] %s check status: %s\n", checkType, checkResult.Status))
	outputBuffer.WriteString(fmt.Sprintf("[wal-verify] %s check details:\n", checkType))

	var outputReader io.Reader
	switch checkType {
	case WalVerifyTimelineCheck:
		outputReader = newTimelineCheckOutputReader(checkResult.Details.(TimelineCheckResult))
	case WalVerifyIntegrityCheck:
		outputReader = newIntegrityCheckOutputReader(checkResult.Details.([]*WalIntegrityScanSegmentSequence))
	default:
		return nil, NewUnknownWalVerifyCheckError(checkType)
	}
	_, err := io.Copy(&outputBuffer, outputReader)
	if err != nil {
		return nil, err
	}
	return &outputBuffer, nil
}

func newIntegrityCheckOutputReader(result []*WalIntegrityScanSegmentSequence) io.Reader {
	var outputBuffer bytes.Buffer

	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(&outputBuffer)
	defer tableWriter.Render()

	tableWriter.AppendHeader(table.Row{"TLI", "Start", "End", "Segments count", "Status"})
	for _, row := range result {
		tableWriter.AppendRow(table.Row{row.TimelineId, row.StartSegment, row.EndSegment, row.SegmentsCount, row.Status})
	}

	return &outputBuffer
}

func newTimelineCheckOutputReader(result TimelineCheckResult) io.Reader {
	var outputBuffer bytes.Buffer

	outputBuffer.WriteString(fmt.Sprintf("Highest timeline found in storage: %d\n",
		result.HighestStorageTimelineId))
	outputBuffer.WriteString(fmt.Sprintf("Current cluster timeline: %d\n",
		result.CurrentTimelineId))

	return &outputBuffer
}
