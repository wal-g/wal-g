package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPgWatcherStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	watcher := NewPgWatcher(ctx, nil, time.Hour)

	cancel()

	select {
	case err, ok := <-watcher.Err:
		require.True(t, ok)
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not return after context cancellation")
	}

	select {
	case _, ok := <-watcher.Err:
		require.False(t, ok, "watcher error channel should close after cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not close its error channel after cancellation")
	}
}
