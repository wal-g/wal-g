package walg

import (
	"math"
	"time"
)

type ExponentialTicker struct {
	MaxRetries int
	retries    int
	MaxWait    float64
	wait       float64
}

func NewExpTicker(retries int, wait float64) *ExponentialTicker {
	return &ExponentialTicker{
		MaxRetries: retries,
		MaxWait:    wait,
	}
}

func (et *ExponentialTicker) Update() {
	if et.wait < et.MaxWait {
		et.wait = math.Exp2(float64(et.retries))
	}
	et.retries+=1
}

func (et *ExponentialTicker) Sleep() {
	time.Sleep(time.Duration(et.wait) * time.Second)
}
