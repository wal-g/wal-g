package tracelog

import (
	"log"
	"os"
	"github.com/pkg/errors"
	"fmt"
	"io/ioutil"
)

var InfoLogger = log.New(os.Stderr, "INFO: ", log.LstdFlags|log.Lmicroseconds)
var WarningLogger = log.New(os.Stderr, "WARNING: ", log.LstdFlags|log.Lmicroseconds)
var ErrorLogger = log.New(os.Stderr, "ERROR: ", log.LstdFlags|log.Lmicroseconds)
var DebugLogger = log.New(ioutil.Discard, "DEBUG: ", log.LstdFlags|log.Lmicroseconds)

const (
	NormalLogLevel = "NORMAL"
	DevelLogLevel  = "DEVEL"
)

var LogLevels = []string{NormalLogLevel, DevelLogLevel}
var logLevel = NormalLogLevel
var logLevelFormatters = map[string]string {
	NormalLogLevel: "%v",
	DevelLogLevel: "%+v",
}

func setupLoggers() {
	if logLevel == NormalLogLevel {
		DebugLogger = log.New(ioutil.Discard, "DEBUG: ", log.LstdFlags|log.Lmicroseconds)
	} else {
		DebugLogger = log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lmicroseconds)
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
