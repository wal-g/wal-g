package binary

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

const (
	catchUpStableSamples     = 2
	convergenceRateTolerance = 0.01
)

type ReplayNotConvergingError struct {
	Head            models.Timestamp
	Processed       models.Timestamp
	Lag             time.Duration
	StorageRate     float64
	ReplayRate      float64
	ObservationTime time.Duration
}

func (e *ReplayNotConvergingError) Error() string {
	return fmt.Sprintf(
		"oplog replay is not converging after %s: storage head %s, processed %s, lag %s, "+
			"storage rate %.2f, replay rate %.2f; initial sync is recommended",
		e.ObservationTime, e.Head, e.Processed, e.Lag, e.StorageRate, e.ReplayRate)
}

type ReplayStalledError struct {
	Head       models.Timestamp
	Processed  models.Timestamp
	StalledFor time.Duration
}

func (e *ReplayStalledError) Error() string {
	return fmt.Sprintf(
		"oplog replay made no progress for %s: storage head %s, processed %s; initial sync is recommended",
		e.StalledFor, e.Head, e.Processed)
}

type catchUpState string

const (
	catchUpStateCatchingUp    catchUpState = "catching-up"
	catchUpStateKeepingUp     catchUpState = "keeping-up"
	catchUpStateFallingBehind catchUpState = "falling-behind"
	catchUpStateStalled       catchUpState = "stalled"
	catchUpStateCaughtUp      catchUpState = "caught-up"
)

type catchUpSample struct {
	at        time.Time
	head      models.Timestamp
	processed models.Timestamp
}

type catchUpAssessment struct {
	state       catchUpState
	lag         time.Duration
	storageRate float64
	replayRate  float64
	eta         time.Duration
	caughtUp    bool
	err         error
}

type catchUpTracker struct {
	config FollowLatestConfig

	samples              []catchUpSample
	startedAt            time.Time
	lastProcessed        models.Timestamp
	lastProgressAt       time.Time
	notConvergingSince   time.Time
	stableCatchUpSamples int
}

func newCatchUpTracker(config FollowLatestConfig) *catchUpTracker {
	return &catchUpTracker{config: config}
}

func (t *catchUpTracker) observe(
	now time.Time,
	followStatus stages.FollowStatus,
	replayStatus stages.ReplayStatus,
) catchUpAssessment {
	if t.startedAt.IsZero() {
		t.startedAt = now
		t.lastProgressAt = now
		t.lastProcessed = replayStatus.ProcessedTS
	}
	if !models.Equal(t.lastProcessed, replayStatus.ProcessedTS) {
		t.lastProcessed = replayStatus.ProcessedTS
		t.lastProgressAt = now
	}

	t.addSample(catchUpSample{at: now, head: followStatus.Head, processed: replayStatus.ProcessedTS})
	lag := timestampLag(followStatus.Head, replayStatus.ProcessedTS)
	storageRate, replayRate, windowReady := t.rates(now)
	assessment := catchUpAssessment{
		state:       catchUpStateKeepingUp,
		lag:         lag,
		storageRate: storageRate,
		replayRate:  replayRate,
	}

	if followStatus.Idle && timestampWithinLag(replayStatus.ProcessedTS, followStatus.Head, t.config.MaxLag) &&
		!replayStatus.PendingTransactions {
		t.stableCatchUpSamples++
	} else {
		t.stableCatchUpSamples = 0
	}
	if t.stableCatchUpSamples >= catchUpStableSamples {
		assessment.state = catchUpStateCaughtUp
		assessment.caughtUp = true
		return assessment
	}

	stalledFor := now.Sub(t.lastProgressAt)
	if stalledFor >= t.config.StallTimeout {
		assessment.state = catchUpStateStalled
		assessment.err = &ReplayStalledError{
			Head:       followStatus.Head,
			Processed:  replayStatus.ProcessedTS,
			StalledFor: stalledFor,
		}
		return assessment
	}

	if !models.LessTS(replayStatus.ProcessedTS, followStatus.Head) {
		t.notConvergingSince = time.Time{}
		return assessment
	}

	convergenceRate := replayRate - storageRate
	if convergenceRate > convergenceRateTolerance {
		assessment.state = catchUpStateCatchingUp
		assessment.eta = durationFromRate(lag, convergenceRate)
		t.notConvergingSince = time.Time{}
		return assessment
	}
	if convergenceRate < -convergenceRateTolerance {
		assessment.state = catchUpStateFallingBehind
	}
	if !windowReady {
		return assessment
	}
	if t.notConvergingSince.IsZero() {
		t.notConvergingSince = now
		return assessment
	}
	observedFor := now.Sub(t.notConvergingSince)
	if observedFor >= t.config.CatchUpTimeout {
		assessment.err = &ReplayNotConvergingError{
			Head:            followStatus.Head,
			Processed:       replayStatus.ProcessedTS,
			Lag:             lag,
			StorageRate:     storageRate,
			ReplayRate:      replayRate,
			ObservationTime: observedFor,
		}
	}
	return assessment
}

func (t *catchUpTracker) addSample(sample catchUpSample) {
	t.samples = append(t.samples, sample)
	cutoff := sample.at.Add(-t.config.ObservationWindow)
	for len(t.samples) > 2 && t.samples[1].at.Before(cutoff) {
		t.samples = t.samples[1:]
	}
}

func (t *catchUpTracker) rates(now time.Time) (float64, float64, bool) {
	if len(t.samples) < 2 {
		return 0, 0, false
	}
	oldest := t.samples[0]
	newest := t.samples[len(t.samples)-1]
	elapsed := newest.at.Sub(oldest.at).Seconds()
	if elapsed <= 0 {
		return 0, 0, false
	}
	storageRate := float64(int64(newest.head.TS)-int64(oldest.head.TS)) / elapsed
	replayRate := float64(int64(newest.processed.TS)-int64(oldest.processed.TS)) / elapsed
	return storageRate, replayRate, now.Sub(t.startedAt) >= t.config.ObservationWindow
}

func (t *catchUpTracker) resetCatchUpConfirmation() {
	t.stableCatchUpSamples = 0
}

type catchUpController struct {
	config   FollowLatestConfig
	progress *stages.ReplayProgress
	fetcher  *stages.FollowingStorageFetcher
}

func newCatchUpController(
	config FollowLatestConfig,
	progress *stages.ReplayProgress,
	fetcher *stages.FollowingStorageFetcher,
) *catchUpController {
	return &catchUpController{config: config, progress: progress, fetcher: fetcher}
}

func (c *catchUpController) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()
	tracker := newCatchUpTracker(c.config)
	var latest stages.FollowStatus
	hasStatus := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case status, ok := <-c.fetcher.Status():
			if !ok {
				return nil
			}
			latest = status
			hasStatus = true
		case now := <-ticker.C:
			if !hasStatus {
				continue
			}
			replayStatus := c.progress.Status()
			assessment := tracker.observe(now, latest, replayStatus)
			logCatchUpAssessment(latest, replayStatus, assessment)
			if assessment.err != nil {
				return assessment.err
			}
			if !assessment.caughtUp {
				continue
			}
			stopped, err := c.fetcher.TryStop(ctx, latest.Head)
			if err != nil {
				return err
			}
			if stopped {
				return nil
			}
			tracker.resetCatchUpConfirmation()
		}
	}
}

func timestampLag(head, processed models.Timestamp) time.Duration {
	if !models.LessTS(processed, head) {
		return 0
	}
	return time.Duration(int64(head.TS)-int64(processed.TS)) * time.Second
}

func timestampWithinLag(processed, head models.Timestamp, maxLag time.Duration) bool {
	if !models.LessTS(processed, head) {
		return true
	}
	if maxLag == 0 {
		return false
	}
	return timestampLag(head, processed) <= maxLag
}

func durationFromRate(lag time.Duration, rate float64) time.Duration {
	if lag <= 0 || rate <= 0 {
		return 0
	}
	seconds := lag.Seconds() / rate
	if math.IsInf(seconds, 0) || math.IsNaN(seconds) || seconds > float64(math.MaxInt64)/float64(time.Second) {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func logCatchUpAssessment(
	followStatus stages.FollowStatus,
	replayStatus stages.ReplayStatus,
	assessment catchUpAssessment,
) {
	eta := "unknown"
	if assessment.eta > 0 {
		eta = assessment.eta.Round(time.Second).String()
	}
	tracelog.InfoLogger.Printf(
		"Oplog replay catch-up: processed=%s durable=%s storage_head=%s lag=%s "+
			"storage_rate=%.2f replay_rate=%.2f state=%s eta=%s",
		replayStatus.ProcessedTS,
		replayStatus.Checkpoint.OpTime.TS,
		followStatus.Head,
		assessment.lag,
		assessment.storageRate,
		assessment.replayRate,
		assessment.state,
		eta,
	)
}
