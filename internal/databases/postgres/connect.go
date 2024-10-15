package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"os"
)

// Connect establishes a connection to postgres using
// a UNIX socket. Must export PGHOST and run with `sudo -E -u postgres`.
// If PGHOST is not set or if the connection fails, an error is returned
// and the connection is `<nil>`.
//
// Example: PGHOST=/var/run/postgresql or PGHOST=10.0.0.1
func Connect(configOptions ...func(config *pgxpool.Config) error) (*pgxpool.Pool, error) {
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		return nil, errors.New("Connect: DATABASE_URL environment variable not set")
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, errors.Wrap(err, "Connect: unable to parse connection string")
	}

	// apply passed custom config options, if any
	for _, option := range configOptions {
		err := option(config)
		if err != nil {
			return nil, err
		}
	}

	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		conn, err = tryConnectToGpSegment(config)

		if err != nil && config.ConnConfig.Host != "localhost" {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided PGHOST and PGPORT, trying localhost:5432")
			config.ConnConfig.Host = "localhost"
			config.ConnConfig.Port = 5432
			conn, err = pgxpool.NewWithConfig(context.Background(), config)
		}

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres connection failed")
		}
	}

	err = checkArchiveCommand(conn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func checkArchiveCommand(conn *pgxpool.Pool) error {
	// TODO: Move this logic to queryRunner

	var standby bool

	err := conn.QueryRow(context.Background(), "select pg_is_in_recovery()").Scan(&standby)
	if err != nil {
		return errors.Wrap(err, "Connect: postgres standby test failed")
	}

	if standby {
		// archive_mode may be configured on primary
		return nil
	}

	var archiveMode string

	err = conn.QueryRow(context.Background(), "show archive_mode").Scan(&archiveMode)

	if err != nil {
		return errors.Wrap(err, "Connect: postgres archive_mode test failed")
	}

	if archiveMode != "on" && archiveMode != "always" {
		tracelog.WarningLogger.Println(
			"It seems your archive_mode is not enabled. This will cause inconsistent backup. " +
				"Please consider configuring WAL archiving.")
	} else {
		var archiveCommand string

		err = conn.QueryRow(context.Background(), "show archive_command").Scan(&archiveCommand)

		if err != nil {
			return errors.Wrap(err, "Connect: postgres archive_mode test failed")
		}

		if len(archiveCommand) == 0 || archiveCommand == "(disabled)" {
			tracelog.WarningLogger.Println(
				"It seems your archive_command is not configured. This will cause inconsistent backup." +
					" Please consider configuring WAL archiving.")
		}
	}
	return nil
}

// nolint:gocritic
func tryConnectToGpSegment(config *pgxpool.Config) (*pgxpool.Pool, error) {
	config.ConnConfig.RuntimeParams["gp_role"] = "utility"
	conn, err := pgxpool.NewWithConfig(context.Background(), config)

	if err != nil {
		config.ConnConfig.RuntimeParams["gp_session_role"] = "utility"
		conn, err = pgxpool.NewWithConfig(context.Background(), config)
	}
	return conn, err
}
