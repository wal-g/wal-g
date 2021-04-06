package internal

import (
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// Connect establishes a connection to postgres using
// a UNIX socket. Must export PGHOST and run with `sudo -E -u postgres`.
// If PGHOST is not set or if the connection fails, an error is returned
// and the connection is `<nil>`.
//
// Example: PGHOST=/var/run/postgresql or PGHOST=10.0.0.1
func Connect(configOptions ...func(config *pgx.ConnConfig) error) (*pgx.Conn, error) {
	config, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, errors.Wrap(err, "Connect: unable to read environment variables")
	}
	// apply passed custom config options, if any
	for _, option := range configOptions {
		err := option(&config)
		if err != nil {
			return nil, err
		}
	}
	conn, err := pgx.Connect(config)
	if err != nil {
		if config.Host != "localhost" {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided PGHOST and PGPORT, trying localhost:5432")
			config.Host = "localhost"
			config.Port = 5432
			conn, err = pgx.Connect(config)
		}

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres connection failed")
		}
	}

	var archiveMode string

	// TODO: Move this logic to queryRunner
	err = conn.QueryRow("show archive_mode").Scan(&archiveMode)

	if err != nil {
		return nil, errors.Wrap(err, "Connect: postgres archive_mode test failed")
	}

	if archiveMode != "on" && archiveMode != "always" {
		tracelog.WarningLogger.Println(
			"It seems your archive_mode is not enabled. This will cause inconsistent backup. " +
				"Please consider configuring WAL archiving.")
	} else {
		var archiveCommand string

		err = conn.QueryRow("show archive_command").Scan(&archiveCommand)

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres archive_mode test failed")
		}

		if len(archiveCommand) == 0 || archiveCommand == "(disabled)" {
			tracelog.WarningLogger.Println(
				"It seems your archive_command is not configured. This will cause inconsistent backup." +
					" Please consider configuring WAL archiving.")
		}
	}

	return conn, nil
}
