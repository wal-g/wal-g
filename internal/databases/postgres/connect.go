package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
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
	config, err := pgx.ParseConfig("")
	if err != nil {
		return nil, errors.Wrap(err, "Connect: unable to read environment variables")
	}

	// apply passed custom config options, if any
	for _, option := range configOptions {
		err := option(config)
		if err != nil {
			return nil, err
		}
	}

	conn, err := pgx.ConnectConfig(context.TODO(), config)
	if err != nil {
		conn, err = tryConnectToGpSegment(config)

		if err != nil && config.Host != "localhost" {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided PGHOST and PGPORT, trying localhost:5432")
			config.Host = "localhost"
			config.Port = 5432
			conn, err = pgx.ConnectConfig(context.TODO(), config)
		}

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres connection failed")
		}
	}

	return conn, nil
}

// nolint:gocritic
func tryConnectToGpSegment(config *pgx.ConnConfig) (*pgx.Conn, error) {
	config.RuntimeParams["gp_role"] = "utility"
	conn, err := pgx.ConnectConfig(context.TODO(), config)

	if err != nil {
		config.RuntimeParams["gp_session_role"] = "utility"
		conn, err = pgx.ConnectConfig(context.TODO(), config)
	}
	return conn, err
}
