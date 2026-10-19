package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
)

func NewPgWatcher(ctx context.Context, queryRunner *PgQueryRunner, aliveCheckInterval time.Duration) *PgAliveWatcher {
	ticker := time.NewTicker(aliveCheckInterval)
	errCh := make(chan error, 1)
	go func() {
		defer ticker.Stop()
		defer close(errCh)
		if err := watchPgStatus(ctx, queryRunner.Ping, ticker.C); err != nil {
			errCh <- err
		}
	}()

	return &PgAliveWatcher{Err: errCh}
}

type PgAliveWatcher struct {
	Err <-chan error
}

func watchPgStatus(ctx context.Context, ping func(context.Context) error, ticks <-chan time.Time) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticks:
		}
		// Both the tick and cancellation may be ready. Do not ping after cleanup.
		if ctx.Err() != nil {
			return nil
		}
		tracelog.DebugLogger.Printf("Checking if Postgres is still alive...")

		err := ping(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to check if the Postgres connection is alive: %w", err)
		}
	}
}
