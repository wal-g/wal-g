package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPgWatcherCancellationClosesWithoutError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	watcher := NewPgWatcher(ctx, nil, time.Hour)
	cancel()
	select {
	case err, ok := <-watcher.Err:
		if ok {
			t.Fatalf("expected a closed channel, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher waited for a tick after cancellation")
	}
}

func TestWatchPgStatusCancellationWithReadyTick(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	err := watchPgStatus(ctx, func(context.Context) error {
		t.Fatal("ping called after cancellation")
		return nil
	}, ticks)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWatchPgStatusCancellationDuringPing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	err := watchPgStatus(ctx, func(ctx context.Context) error {
		cancel()
		return ctx.Err()
	}, ticks)
	if err != nil {
		t.Fatalf("normal cancellation reported as failure: %v", err)
	}
}

func TestWatchPgStatusReportsPingFailure(t *testing.T) {
	want := errors.New("connection lost")
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	err := watchPgStatus(t.Context(), func(context.Context) error { return want }, ticks)
	if !errors.Is(err, want) {
		t.Fatalf("expected connection failure, got %v", err)
	}
}

func TestWatchPgStatusSuccessfulPingThenCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	err := watchPgStatus(ctx, func(context.Context) error {
		cancel()
		return nil
	}, ticks)
	if err != nil {
		t.Fatal(err)
	}
}
