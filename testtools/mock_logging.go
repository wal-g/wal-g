package testtools

type callingFatalOnErrorFuncStats struct {
	FatalOnErrorCallsCount int
	Err                    error
}

type callingPrintFuncStats struct {
	PrintLnCallsCount int
	PrintMsg          string
}

type infoLoggerMock struct {
	Stats *callingPrintFuncStats
}

func (loggerMock infoLoggerMock) Println(v ...interface{}) {
	loggerMock.Stats.PrintLnCallsCount++
	loggerMock.Stats.PrintMsg = v[0].(string)
}

type errorLoggerMock struct {
	Stats *callingFatalOnErrorFuncStats
}

func (loggerMock errorLoggerMock) FatalOnError(err error) {
	loggerMock.Stats.FatalOnErrorCallsCount++
	loggerMock.Stats.Err = err
}

func MockLoggers() (infoLoggerMock, errorLoggerMock) {
	infoStats := callingPrintFuncStats{
		PrintLnCallsCount: 0,
		PrintMsg:          "",
	}
	errorStats := callingFatalOnErrorFuncStats{
		FatalOnErrorCallsCount: 0,
		Err:                    nil,
	}

	return infoLoggerMock{Stats: &infoStats},
		errorLoggerMock{Stats: &errorStats}
}
