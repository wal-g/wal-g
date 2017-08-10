package walg

import (
	"math"
	"time"
)

// ExponentialTicker is used for exponential backoff
// for uploading to S3. If the max wait time is reached,
// retries will occur after max wait time intervals up to
// max retries.
type ExponentialTicker struct {
	MaxRetries int
	retries    int
	MaxWait    float64
	wait       float64
}

// NewExpTicker creates a new ExponentialTicker with
// configurable max number of retries and max wait time.
func NewExpTicker(retries int, wait float64) *ExponentialTicker {
	return &ExponentialTicker{
		MaxRetries: retries,
		MaxWait:    wait,
	}
}

// Update increases running count of retries by 1 and
// exponentially increases the wait time until the
// max wait time is reached.
func (et *ExponentialTicker) Update() {
	if et.wait < et.MaxWait {
		et.wait = math.Exp2(float64(et.retries))
	}
	et.retries++
}

// Sleep will wait in seconds.
func (et *ExponentialTicker) Sleep() {
	time.Sleep(time.Duration(et.wait) * time.Second)
}
