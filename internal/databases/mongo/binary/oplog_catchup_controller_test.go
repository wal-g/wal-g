package binary

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

func TestCatchUpTrackerRequiresStableIdleHead(t *testing.T) {
	tracker := newCatchUpTracker(followTestConfig())
	now := time.Now()
	status := stages.FollowStatus{Head: models.Timestamp{TS: 100}, Idle: true}
	progress := stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 100}}

	first := tracker.observe(now, status, progress)
	second := tracker.observe(now.Add(time.Second), status, progress)

	require.False(t, first.caughtUp)
	require.True(t, second.caughtUp)
	require.Equal(t, catchUpStateCaughtUp, second.state)
}

func TestCatchUpTrackerDoesNotStopWithPendingTransaction(t *testing.T) {
	config := followTestConfig()
	config.StallTimeout = 5 * time.Second
	tracker := newCatchUpTracker(config)
	now := time.Now()
	status := stages.FollowStatus{Head: models.Timestamp{TS: 100}, Idle: true}
	progress := stages.ReplayStatus{
		ProcessedTS:         models.Timestamp{TS: 100},
		PendingTransactions: true,
	}

	tracker.observe(now, status, progress)
	assessment := tracker.observe(now.Add(time.Second), status, progress)

	require.False(t, assessment.caughtUp)
	require.NoError(t, assessment.err)

	assessment = tracker.observe(now.Add(5*time.Second), status, progress)
	var stalledErr *ReplayStalledError
	require.ErrorAs(t, assessment.err, &stalledErr)
}

func TestCatchUpTrackerEstimatesConvergence(t *testing.T) {
	tracker := newCatchUpTracker(followTestConfig())
	now := time.Now()
	tracker.observe(now,
		stages.FollowStatus{Head: models.Timestamp{TS: 200}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 100}})

	assessment := tracker.observe(now.Add(10*time.Second),
		stages.FollowStatus{Head: models.Timestamp{TS: 210}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 130}})

	require.Equal(t, catchUpStateCatchingUp, assessment.state)
	require.InDelta(t, 1, assessment.storageRate, 0.001)
	require.InDelta(t, 3, assessment.replayRate, 0.001)
	require.Equal(t, 40*time.Second, assessment.eta)
}

func TestCatchUpTrackerReportsNonConvergingReplay(t *testing.T) {
	config := followTestConfig()
	config.ObservationWindow = 10 * time.Second
	config.CatchUpTimeout = 20 * time.Second
	tracker := newCatchUpTracker(config)
	now := time.Now()
	tracker.observe(now,
		stages.FollowStatus{Head: models.Timestamp{TS: 200}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 100}})
	tracker.observe(now.Add(10*time.Second),
		stages.FollowStatus{Head: models.Timestamp{TS: 210}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 110}})

	assessment := tracker.observe(now.Add(30*time.Second),
		stages.FollowStatus{Head: models.Timestamp{TS: 230}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 130}})

	var convergenceErr *ReplayNotConvergingError
	require.ErrorAs(t, assessment.err, &convergenceErr)
	require.Equal(t, 100*time.Second, convergenceErr.Lag)
}

func TestCatchUpTrackerDetectsGrowingLag(t *testing.T) {
	tracker := newCatchUpTracker(followTestConfig())
	now := time.Now()
	tracker.observe(now,
		stages.FollowStatus{Head: models.Timestamp{TS: 200}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 100}})

	assessment := tracker.observe(now.Add(10*time.Second),
		stages.FollowStatus{Head: models.Timestamp{TS: 220}},
		stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 110}})

	require.Equal(t, catchUpStateFallingBehind, assessment.state)
	require.InDelta(t, 2, assessment.storageRate, 0.001)
	require.InDelta(t, 1, assessment.replayRate, 0.001)
}

func TestCatchUpTrackerReportsStalledReplay(t *testing.T) {
	config := followTestConfig()
	config.StallTimeout = 5 * time.Second
	tracker := newCatchUpTracker(config)
	now := time.Now()
	status := stages.FollowStatus{Head: models.Timestamp{TS: 200}}
	progress := stages.ReplayStatus{ProcessedTS: models.Timestamp{TS: 100}}
	tracker.observe(now, status, progress)

	assessment := tracker.observe(now.Add(5*time.Second), status, progress)

	var stalledErr *ReplayStalledError
	require.ErrorAs(t, assessment.err, &stalledErr)
}

func followTestConfig() FollowLatestConfig {
	return FollowLatestConfig{
		PollInterval:      time.Second,
		ObservationWindow: time.Minute,
		CatchUpTimeout:    time.Minute,
		StallTimeout:      time.Hour,
	}
}
