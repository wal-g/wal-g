package binary

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

func TestSuperviseOplogReplayCarriesDurableOpTimeIntoNextAttempt(t *testing.T) {
	progress := stages.NewReplayProgress(models.Timestamp{TS: 100, Inc: 1})
	durable := models.OpTime{TS: models.Timestamp{TS: 101, Inc: 2}, Term: 7}
	attempts := 0

	err := superviseOplogReplay(t.Context(), ReplyOplogConfig{MaxMongodRestarts: 1}, progress,
		func(attempt oplogReplayAttempt) replayAttemptResult {
			attempts++
			if attempts == 1 {
				require.False(t, attempt.resumeAfter)
				progress.MarkDurable(durable)
				return replayAttemptResult{kind: replayAttemptMongodExited, err: errors.New("crash")}
			}
			require.True(t, attempt.resumeAfter)
			require.Equal(t, durable, attempt.checkpoint.OpTime)
			return replayAttemptResult{kind: replayAttemptCompleted}
		})

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

func TestSuperviseOplogReplayStopsAfterConsecutiveCrashes(t *testing.T) {
	progress := stages.NewReplayProgress(models.Timestamp{TS: 100, Inc: 1})
	attempts := 0

	err := superviseOplogReplay(t.Context(), ReplyOplogConfig{MaxMongodRestarts: 2}, progress,
		func(oplogReplayAttempt) replayAttemptResult {
			attempts++
			return replayAttemptResult{kind: replayAttemptMongodExited, err: errors.New("crash")}
		})

	require.ErrorContains(t, err, "crashed after 2 restarts")
	require.Equal(t, 3, attempts)
}

func TestSuperviseOplogReplayStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	progress := stages.NewReplayProgress(models.Timestamp{TS: 100, Inc: 1})
	attempts := 0

	err := superviseOplogReplay(ctx, ReplyOplogConfig{MaxMongodRestarts: 5}, progress,
		func(oplogReplayAttempt) replayAttemptResult {
			attempts++
			cancel()
			return replayAttemptResult{kind: replayAttemptMongodExited, err: errors.New("killed")}
		})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

func TestReplayAttemptExecutorClassifiesStartupCrash(t *testing.T) {
	processErr := errors.New("process crashed")
	mongod := newFakeSupervisedMongod(processErr)
	mongod.connectErr = errors.New("connect failed")
	mongod.exit()
	executor := replayAttemptExecutor{
		startMongod: func(context.Context, string, ReplyOplogConfig) (supervisedMongod, error) {
			return mongod, nil
		},
		replay: func(context.Context, string, ReplyOplogConfig, oplogReplayAttempt) error {
			t.Fatal("replay must not start after a startup crash")
			return nil
		},
		exitDetectionDelay: time.Millisecond,
	}

	result := executor.run(t.Context(), ReplyOplogConfig{}, oplogReplayAttempt{}, "config")

	require.Equal(t, replayAttemptMongodExited, result.kind)
	require.ErrorIs(t, result.err, processErr)
}

type fakeSupervisedMongod struct {
	mu         sync.Mutex
	done       chan struct{}
	waitErr    error
	connectErr error
}

func newFakeSupervisedMongod(waitErr error) *fakeSupervisedMongod {
	return &fakeSupervisedMongod{done: make(chan struct{}), waitErr: waitErr}
}

func (m *fakeSupervisedMongod) URI() string {
	return "mongodb://localhost:27017"
}

func (m *fakeSupervisedMongod) Done() <-chan struct{} {
	return m.done
}

func (m *fakeSupervisedMongod) Wait() error {
	<-m.done
	return m.waitErr
}

func (m *fakeSupervisedMongod) Connect(context.Context) error {
	return m.connectErr
}

func (m *fakeSupervisedMongod) Shutdown(context.Context) error {
	m.exit()
	return nil
}

func (m *fakeSupervisedMongod) Stop() {
	m.exit()
}

func (m *fakeSupervisedMongod) Close() {
	m.exit()
}

func (m *fakeSupervisedMongod) exit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.done:
	default:
		close(m.done)
	}
}
