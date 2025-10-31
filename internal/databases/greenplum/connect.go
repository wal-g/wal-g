package greenplum

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

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

// Postgres connection, but with possibility to connect to GP
func Connect(configOptions ...func(config *pgx.ConnConfig) error) (*pgx.Conn, error) {
	conn, err := postgres.Connect(configOptions...)
	if err != nil {
		// Parse config from environment to try GP-specific connection
		config, configErr := pgx.ParseConfig("")
		if configErr != nil {
			return nil, errors.Wrap(configErr, "Connect: unable to read environment variables")
		}

		// apply passed custom config options, if any
		for _, option := range configOptions {
			err := option(config)
			if err != nil {
				return nil, err
			}
		}

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
