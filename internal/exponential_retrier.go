package internal

import "time"

type ExponentialRetrier struct {
	sleepDuration      time.Duration
	sleepDurationBound time.Duration
}

func NewExponentialRetrier(startSleepDuration, sleepDurationBound time.Duration) *ExponentialRetrier {
	return &ExponentialRetrier{startSleepDuration, sleepDurationBound}
}

func (retrier *ExponentialRetrier) retry() {
	time.Sleep(retrier.sleepDuration)
	retrier.sleepDuration *= 2
	if retrier.sleepDuration > retrier.sleepDurationBound {
		retrier.sleepDuration = retrier.sleepDurationBound
	}
}
