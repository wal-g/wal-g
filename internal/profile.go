package internal

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/pkg/profile"
)

type ProfileStopper interface {
	Stop()
}

func Profile() (ProfileStopper, error) {
	envSamplingRatio := os.Getenv("PROFILE_SAMPLING_RATIO")
	if envSamplingRatio == "" {
		return nil, nil
	}

	samplingRatio, err := strconv.ParseFloat(envSamplingRatio, 64)
	if err != nil {
		return nil, fmt.Errorf("could not parse PROFILE_SAMPLING_RATIO as float: %v", err)
	}

	// sample profiling invoked commands
	rand.Seed(time.Now().UnixNano())
	if rand.Float64() >= samplingRatio {
		return nil, nil
	}

	var opts []func(*profile.Profile)

	profileMode := os.Getenv("PROFILE_MODE")
	switch profileMode {
	case "cpu":
		opts = append(opts, profile.CPUProfile)
	case "mem":
		opts = append(opts, profile.MemProfile)
	case "mutex":
		opts = append(opts, profile.MutexProfile)
	case "block":
		opts = append(opts, profile.BlockProfile)
	case "threadcreation":
		opts = append(opts, profile.ThreadcreationProfile)
	case "trace":
		opts = append(opts, profile.TraceProfile)
	case "goroutine":
		opts = append(opts, profile.GoroutineProfile)
	}

	profilePath := os.Getenv("PROFILE_PATH")
	if profilePath != "" {
		opts = append(opts, profile.ProfilePath(profilePath))
	}

	p := profile.Start(opts...)

	return p, nil
}
