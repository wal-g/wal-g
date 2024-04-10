package internal

import (
	"math/rand"
	"time"

	conf "github.com/wal-g/wal-g/internal/config"

	"github.com/pkg/profile"
	"github.com/spf13/viper"
)

type ProfileStopper interface {
	Stop()
}

func Profile() (ProfileStopper, error) {
	if !viper.IsSet(conf.ProfileSamplingRatio) {
		return nil, nil
	}

	samplingRatio := viper.GetFloat64(conf.ProfileSamplingRatio)

	// sample profiling invoked commands
	rand.Seed(time.Now().UnixNano())
	if rand.Float64() >= samplingRatio {
		return nil, nil
	}

	var opts []func(*profile.Profile)

	profileMode := viper.GetString(conf.ProfileMode)
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

	profilePath := viper.GetString(conf.ProfilePath)
	if profilePath != "" {
		opts = append(opts, profile.ProfilePath(profilePath))
	}

	p := profile.Start(opts...)

	return p, nil
}
