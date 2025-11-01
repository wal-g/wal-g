package greenplum

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// GpConnectOption returns a connection config option that sets GP utility mode
// This is required for connecting to GP/Cloudberry segments
func GpConnectOption() func(*pgx.ConnConfig) error {
	return func(config *pgx.ConnConfig) error {
		if config.RuntimeParams == nil {
			config.RuntimeParams = make(map[string]string)
		}
		// Try gp_role first (newer versions)
		config.RuntimeParams["gp_role"] = "utility"
		// Also set gp_session_role as fallback (older versions)
		config.RuntimeParams["gp_session_role"] = "utility"
		return nil
	}
}

// Connect establishes connection to Greenplum master with GP-specific options
// and additional fallback logic (tries localhost:5432 as last resort)
func Connect(configOptions ...func(config *pgx.ConnConfig) error) (*pgx.Conn, error) {
	// Combine user options with GP utility mode option
	allOptions := append([]func(*pgx.ConnConfig) error{GpConnectOption()}, configOptions...)

	// Try normal connection with GP options
	conn, err := postgres.Connect(allOptions...)
	if err != nil {
		// Additional fallback for GP master: try localhost:5432
		config, configErr := pgx.ParseConfig("")
		if configErr != nil {
			return nil, errors.Wrap(configErr, "Connect: unable to read environment variables")
		}

		// apply all options including GP utility mode
		for _, option := range allOptions {
			optErr := option(config)
			if optErr != nil {
				return nil, optErr
			}
		}

		if config.Host != "localhost" {
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
