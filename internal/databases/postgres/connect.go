package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
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

	err = checkArchiveCommand(conn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func ConvertPgConnConfigToString(config *pgconn.Config) string {
	if config == nil {
		return ""
	}

	// Build the connection string manually
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.Database)

	// Append any additional parameters to the connection string
	if len(config.RuntimeParams) > 0 {
		params := "?"
		for key, value := range config.RuntimeParams {
			params += fmt.Sprintf("%s=%s&", key, value)
		}
		// Remove the trailing "&" if any runtime parameters exist
		connStr += params[:len(params)-1]
	}

	return connStr
}

func checkArchiveCommand(conn *pgx.Conn) error {
	// TODO: Move this logic to queryRunner

	var standby bool

	err := conn.QueryRow(context.TODO(), "select pg_is_in_recovery()").Scan(&standby)
	if err != nil {
		return errors.Wrap(err, "Connect: postgres standby test failed")
	}

	if standby {
		// archive_mode may be configured on primary
		return nil
	}

	var archiveMode string

	err = conn.QueryRow(context.TODO(), "show archive_mode").Scan(&archiveMode)

	if err != nil {
		return errors.Wrap(err, "Connect: postgres archive_mode test failed")
	}

	if archiveMode != "on" && archiveMode != "always" {
		tracelog.WarningLogger.Println(
			"It seems your archive_mode is not enabled. This will cause inconsistent backup. " +
				"Please consider configuring WAL archiving.")
	} else {
		var archiveCommand string

		err = conn.QueryRow(context.TODO(), "show archive_command").Scan(&archiveCommand)

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
func tryConnectToGpSegment(config *pgx.ConnConfig) (*pgx.Conn, error) {
	config.RuntimeParams["gp_role"] = "utility"
	conn, err := pgx.ConnectConfig(context.TODO(), config)

	if err != nil {
		config.RuntimeParams["gp_session_role"] = "utility"
		conn, err = pgx.ConnectConfig(context.TODO(), config)
	}
	return conn, err
}
