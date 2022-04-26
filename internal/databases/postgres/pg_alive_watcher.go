package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx"

	"github.com/wal-g/tracelog"
)

func NewPgWatcher(conn *pgx.Conn, aliveCheckInterval time.Duration) *PgAliveWatcher {
	ticker := time.NewTicker(aliveCheckInterval)
	errCh := make(chan error, 1)
	go func() {
		errCh <- watchPgStatus(conn, ticker)
		close(errCh)
	}()

	return &PgAliveWatcher{Err: errCh}
}

type PgAliveWatcher struct {
	Err <-chan error
}

func watchPgStatus(conn *pgx.Conn, ticker *time.Ticker) error {
	for {
		<-ticker.C
		tracelog.DebugLogger.Printf("Checking if Postgres is still alive...")

		ctx := context.Background()
		err := conn.Ping(ctx)
		if err != nil {
			return fmt.Errorf("failed to check if the Postgres connection is alive: %v", err)
		}
	}
}
