package walg

import (
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"log"
)

// Connect establishes a connection to postgres using
// a UNIX socket. Must export PGHOST and run with `sudo -E -u postgres`.
// If PGHOST is not set or if the connection fails, an error is returned
// and the connection is `<nil>`.
//
// Example: PGHOST=/var/run/postgresql or PGHOST=10.0.0.1
func Connect() (*pgx.Conn, error) {
	config, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, errors.Wrap(err, "Connect: unable to read environment variables")
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return nil, errors.Wrap(err, "Connect: postgres connection failed")
	}

	var archiveMode string

	// TODO: Move this logic to queryRunner
	err = conn.QueryRow("show archive_mode").Scan(&archiveMode)

	if err != nil {
		return nil, errors.Wrap(err, "Connect: postgres archive_mode test failed")
	}

	if archiveMode != "on" && archiveMode != "always" {
		log.Println("WARNING! It seems your archive_mode is not enabled. This will cause inconsistent backup. Please consider configuring WAL archiving.")
	} else {
		var archiveCommand string

		err = conn.QueryRow("show archive_command").Scan(&archiveCommand)

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres archive_mode test failed")
		}

		if len(archiveCommand) == 0 || archiveCommand == "(disabled)" {
			log.Println("WARNING! It seems your archive_command is not configured. This will cause inconsistent backup. Please consider configuring WAL archiving.")
		}
	}

	return conn, nil
}
