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

func (logger *errorLogger) PrintError(err error) {
	logger.Printf(GetErrorFormatter()+"\n", err)
}
