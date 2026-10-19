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
		defer close(errCh)
		defer ticker.Stop()
		errCh <- watchPgStatus(ctx, queryRunner, ticker)
	}()

	return &PgAliveWatcher{Err: errCh}
}

type PgAliveWatcher struct {
	Err <-chan error
}

func watchPgStatus(ctx context.Context, queryRunner *PgQueryRunner, ticker *time.Ticker) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			tracelog.DebugLogger.Printf("Checking if Postgres is still alive...")

			err := queryRunner.Ping(ctx)
			if err != nil {
				return fmt.Errorf("failed to check if the Postgres connection is alive: %v", err)
			}
		}
	}
}
