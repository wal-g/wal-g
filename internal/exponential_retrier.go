package internal

import "time"

type ExponentialSleeper struct {
	sleepDuration      time.Duration
	sleepDurationBound time.Duration
}

func NewExponentialSleeper(startSleepDuration, sleepDurationBound time.Duration) *ExponentialSleeper {
	return &ExponentialSleeper{startSleepDuration, sleepDurationBound}
}

func (retrier *ExponentialSleeper) Sleep() {
	time.Sleep(retrier.sleepDuration)
	retrier.sleepDuration *= 2
	if retrier.sleepDuration > retrier.sleepDurationBound {
		retrier.sleepDuration = retrier.sleepDurationBound
	}
}
