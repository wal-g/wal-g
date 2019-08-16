package tracelog

import (
	"io"
	"log"
)

type errorLogger struct {
	*log.Logger
}

func NewErrorLogger(out io.Writer, prefix string) *errorLogger {
	return &errorLogger{log.New(out, prefix, timeFlags)}
}

func (logger *errorLogger) FatalError(err error) {
	logger.Fatalf(GetErrorFormatter(), err)
}

func (logger *errorLogger) FatalfOnError(format string, err error) {
	if err != nil {
		logger.Fatalf(format, err)
	}
}

func (logger *errorLogger) FatalOnError(err error) {
	if err != nil {
		logger.FatalError(err)
	}
}

func (logger *errorLogger) PrintError(err error) {
	logger.Printf(GetErrorFormatter()+"\n", err)
}
