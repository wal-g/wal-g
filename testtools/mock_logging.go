package testtools

type callingFatalOnErrorFuncStats struct {
	FatalOnErrorCallsCount int
	Err                    error
}

type callingPrintFuncStats struct {
	PrintLnCallsCount int
	PrintMsg          string
}

type InfoLoggerMock struct {
	Stats *callingPrintFuncStats
}

func (loggerMock InfoLoggerMock) Println(v ...interface{}) {
	loggerMock.Stats.PrintLnCallsCount++
	loggerMock.Stats.PrintMsg = v[0].(string)
}

type ErrorLoggerMock struct {
	Stats *callingFatalOnErrorFuncStats
}

func (loggerMock ErrorLoggerMock) FatalOnError(err error) {
	loggerMock.Stats.FatalOnErrorCallsCount++
	loggerMock.Stats.Err = err
}

func MockLoggers() (InfoLoggerMock, ErrorLoggerMock) {
	infoStats := callingPrintFuncStats{
		PrintLnCallsCount: 0,
		PrintMsg:          "",
	}
	errorStats := callingFatalOnErrorFuncStats{
		FatalOnErrorCallsCount: 0,
		Err:                    nil,
	}

	return InfoLoggerMock{Stats: &infoStats},
		ErrorLoggerMock{Stats: &errorStats}
}
