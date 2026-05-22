package internal

import "time"

type ExponentialSleeper struct {
	sleepDuration      time.Duration
	sleepDurationBound time.Duration
}

func NewExponentialSleeper(startSleepDuration, sleepDurationBound time.Duration) *ExponentialSleeper {
	return &ExponentialSleeper{startSleepDuration, sleepDurationBound}
}

func (sleeper *ExponentialSleeper) Sleep() {
	time.Sleep(sleeper.sleepDuration)
	sleeper.sleepDuration = min(sleeper.sleepDuration*2, sleeper.sleepDurationBound)
}
