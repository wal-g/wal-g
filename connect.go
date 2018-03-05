package walg

import (
	"log"
	"regexp"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
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

	var archive_mode string

	// TODO: Move this logic to queryRunner
	err = conn.QueryRow("show archive_mode").Scan(&archive_mode)

	if err != nil {
		return nil, errors.Wrap(err, "Connect: postgres archive_mode test failed")
	}

	if archive_mode != "on" && archive_mode != "always" {
		log.Println("WARNING! It seems your archive_mode is not enabled. This will cause inconsistent backup. Please consider configuring WAL archiving.")
	} else {
		var archive_command string

		err = conn.QueryRow("show archive_command").Scan(&archive_command)

		if err != nil {
			return nil, errors.Wrap(err, "Connect: postgres archive_mode test failed")
		}

		if len(archive_command) == 0 || archive_command == "(disabled)" {
			log.Println("WARNING! It seems your archive_command is not configured. This will cause inconsistent backup. Please consider configuring WAL archiving.")
		}
	}

	return conn, nil
}

// StartBackup starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty string and an error if backup
// fails.
func (b *Bundle) StartBackup(conn *pgx.Conn, backup string) (backupName string, lsn uint64, version int, err error) {
	var name, lsnStr string
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return "", 0, queryRunner.Version, errors.Wrap(err, "StartBackup: Failed to build query runner.")
	}
	name, lsnStr, b.Replica, err = queryRunner.StartBackup(backup)

	lsn, err = ParseLsn(lsnStr)
	if err != nil {
		return "", 0, queryRunner.Version, err
	}

	if b.Replica {
		name, b.Timeline, err = WALFileName(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, err
		}
	}
	return backupNamePrefix + name, lsn, queryRunner.Version, nil

}

const backupNamePrefix = "base_"

func (b *Bundle) CheckTimelineChanged(conn *pgx.Conn) bool {
	if b.Replica {
		timeline, err := readTimeline(conn)
		if err != nil {
			log.Printf("Unbale to check timeline change. Sentinel for the backup will not be uploaded.")
			return true
		}

		// Per discussion in
		// https://www.postgresql.org/message-id/flat/BF2AD4A8-E7F5-486F-92C8-A6959040DEB6%40yandex-team.ru#BF2AD4A8-E7F5-486F-92C8-A6959040DEB6@yandex-team.ru
		// Following check is the very pessimistic approach on replica backup invalidation
		if timeline != b.Timeline {
			log.Printf("Timeline has changed since backup start. Sentinel for the backup will not be uploaded.")
			return true
		}
	}
	return false
}

// FormatName grabs the name of the WAL file and returns it in the form of `base_...`.
// If no match is found, returns an empty string and a `NoMatchAvailableError`.
func FormatName(s string) (string, error) {
	re := regexp.MustCompile(`\(([^\)]+)\)`)
	f := re.FindString(s)
	if f == "" {
		return "", errors.Wrap(NoMatchAvailableError{s}, "FormatName:")
	}
	return backupNamePrefix + f[6:len(f)-1], nil
}
