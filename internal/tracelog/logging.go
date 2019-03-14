package tracelog

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/pkg/errors"
)

const (
	NormalLogLevel = "NORMAL"
	DevelLogLevel  = "DEVEL"
	timeFlags      = log.LstdFlags | log.Lmicroseconds
)

var InfoLogger = NewErrorLogger(os.Stderr, "INFO: ")
var WarningLogger = NewErrorLogger(os.Stderr, "WARNING: ")
var ErrorLogger = NewErrorLogger(os.Stderr, "ERROR: ")
var DebugLogger = NewErrorLogger(ioutil.Discard, "DEBUG: ")

var LogLevels = []string{NormalLogLevel, DevelLogLevel}
var logLevel = NormalLogLevel
var logLevelFormatters = map[string]string{
	NormalLogLevel: "%v",
	DevelLogLevel:  "%+v",
}

func setupLoggers() {
	if logLevel == NormalLogLevel {
		DebugLogger = NewErrorLogger(ioutil.Discard, "DEBUG: ")
	} else {
		DebugLogger = NewErrorLogger(os.Stderr, "DEBUG: ")
	}
}

type LogLevelError struct {
	error
}

func NewLogLevelError() LogLevelError {
	return LogLevelError{errors.Errorf("got incorrect log level: '%s', expected one of: '%v'", logLevel, LogLevels)}
}

func (err LogLevelError) Error() string {
	return fmt.Sprintf(GetErrorFormatter(), err.error)
}

func GetErrorFormatter() string {
	return logLevelFormatters[logLevel]
}

func UpdateLogLevel(newLevel string) error {
	isCorrect := false
	for _, level := range LogLevels {
		if newLevel == level {
			isCorrect = true
		}
	}
	if !isCorrect {
		return NewLogLevelError()
	}

	logLevel = newLevel
	setupLoggers()
	return nil
}
