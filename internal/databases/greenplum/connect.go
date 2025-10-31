package greenplum

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// Connect establishes connection to Greenplum master with additional fallback logic
// (tries localhost:5432 as last resort)
func Connect(configOptions ...func(config *pgx.ConnConfig) error) (*pgx.Conn, error) {
	// Try normal connection (postgres.Connect already handles GP segment fallback)
	conn, err := postgres.Connect(configOptions...)
	if err != nil {
		// Additional fallback for GP master: try localhost:5432
		config, configErr := pgx.ParseConfig("")
		if configErr != nil {
			return nil, errors.Wrap(configErr, "Connect: unable to read environment variables")
		}

		// apply passed custom config options, if any
		for _, option := range configOptions {
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
			
			// Try with GP utility mode first
			config.RuntimeParams["gp_role"] = "utility"
			conn, err = pgx.ConnectConfig(context.TODO(), config)
			if err != nil {
				config.RuntimeParams["gp_session_role"] = "utility"
				conn, err = pgx.ConnectConfig(context.TODO(), config)
			}
		}

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres connection failed")
		}
	}

	return conn, nil
}
