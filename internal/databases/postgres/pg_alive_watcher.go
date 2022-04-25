package postgres

import (
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
)

func NewPgWatcher(aliveCheckInterval time.Duration) *PgAliveWatcher {
	ticker := time.NewTicker(aliveCheckInterval)
	errCh := make(chan error, 1)
	go func() {
		errCh <- watchPgStatus(ticker)
		close(errCh)
	}()

	return &PgAliveWatcher{Err: errCh}
}

type PgAliveWatcher struct {
	Err <-chan error
}

func watchPgStatus(ticker *time.Ticker) error {
	for {
		<-ticker.C
		tracelog.DebugLogger.Printf("Checking if Postgres is still alive...")
		conn, err := Connect()
		if err != nil {
			return fmt.Errorf("failed to connect to Postgres: %v", err)
		}

		err = conn.Close()
		if err != nil {
			tracelog.WarningLogger.Printf("watchPgStatus: failed to disconnect: %v", err)
		}
	}
}
