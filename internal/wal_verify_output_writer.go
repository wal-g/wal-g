package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

	checkDetails, err := checkResult.Details.NewPlainTextReader()
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(&outputBuffer, checkDetails)
	if err != nil {
		return nil, err
	}
	return &outputBuffer, nil
}
