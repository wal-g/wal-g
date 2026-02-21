package postgres

import (
	"fmt"
	"log/slog"
	"time"
)

func NewPgWatcher(queryRunner *PgQueryRunner, aliveCheckInterval time.Duration) *PgAliveWatcher {
	ticker := time.NewTicker(aliveCheckInterval)
	errCh := make(chan error, 1)
	go func() {
		errCh <- watchPgStatus(queryRunner, ticker)
		close(errCh)
	}()

	return &PgAliveWatcher{Err: errCh}
}

type PgAliveWatcher struct {
	Err <-chan error
}

func watchPgStatus(queryRunner *PgQueryRunner, ticker *time.Ticker) error {
	for {
		<-ticker.C
		slog.Debug(fmt.Sprintf("Checking if Postgres is still alive..."))

		err := queryRunner.Ping()
		if err != nil {
			return fmt.Errorf("failed to check if the Postgres connection is alive: %v", err)
		}
	}
}
