package greenplum

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// ConnectSegment establishes a connection to a GP/Cloudberry segment
// It tries multiple connection strategies:
// 1. Normal Postgres connection
// 2. GP utility mode with gp_role
// 3. GP utility mode with gp_session_role (older versions)
func ConnectSegment(configOptions ...func(config *pgx.ConnConfig) error) (*pgx.Conn, error) {
	// Try normal Postgres connection first
	conn, err := postgres.Connect(configOptions...)
	if err == nil {
		return conn, nil
	}

	// Try with gp_role=utility (newer GP/CB versions)
	gpRoleOption := func(config *pgx.ConnConfig) error {
		if config.RuntimeParams == nil {
			config.RuntimeParams = make(map[string]string)
		}
		config.RuntimeParams["gp_role"] = "utility"
		return nil
	}
	allOptions := append([]func(*pgx.ConnConfig) error{gpRoleOption}, configOptions...)
	conn, err = postgres.Connect(allOptions...)
	if err == nil {
		return conn, nil
	}

	// Try with gp_session_role=utility (older GP versions)
	gpSessionRoleOption := func(config *pgx.ConnConfig) error {
		if config.RuntimeParams == nil {
			config.RuntimeParams = make(map[string]string)
		}
		config.RuntimeParams["gp_session_role"] = "utility"
		return nil
	}
	allOptions = append([]func(*pgx.ConnConfig) error{gpSessionRoleOption}, configOptions...)
	return postgres.Connect(allOptions...)
}

// Connect establishes connection to Greenplum master with GP-specific options
// and additional fallback logic (tries localhost:5432 as last resort)
func Connect(configOptions ...func(config *pgx.ConnConfig) error) (*pgx.Conn, error) {
	// Try ConnectSegment which handles GP utility mode properly
	conn, err := ConnectSegment(configOptions...)
	if err != nil {
		// Additional fallback for GP master: try localhost:5432
		config, configErr := pgx.ParseConfig("")
		if configErr != nil {
			return nil, errors.Wrap(configErr, "Connect: unable to read environment variables")
		}

		// apply custom options
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

			// Try with gp_role for localhost
			config.RuntimeParams = make(map[string]string)
			config.RuntimeParams["gp_role"] = "utility"
			conn, err = pgx.ConnectConfig(context.TODO(), config)
			if err != nil {
				// Try with gp_session_role
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
