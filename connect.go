package walg

import (
	"regexp"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"path"
)

// Connect establishes a connection to postgres using
// a UNIX socket. Must export PGHOST and run with `sudo -E -u postgres`.
// If PGHOST is not set or if the connection fails, an error is returned
// and the connection is `<nil>`.
func Connect() (*pgx.Conn, error) {
	config, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, errors.Wrap(err, "Connect: unable to read environment variables")
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return nil, errors.Wrap(err, "Connect: postgres connection failed")
	}

	return conn, nil
}

// StartBackup starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty string and an error if backup
// fails.
func StartBackup(conn *pgx.Conn, backup string, backupDir string) (string, error) {
	var name, lsnStr string
	var replica bool
	var version int
	// We extract here version since it is not used elsewhere. If reused, this should be refactored.
	// TODO: implement offline backups, incapsulate PostgreSQL version logic and create test specs for this logic.
	// Currently all version-dependent logic is here
	err := conn.QueryRow("select (current_setting('server_version_num'))::int").Scan(&version)
	if err != nil {
		return "", errors.Wrap(err, "QueryFile: getting Postgres version failed")
	}
	walname := "xlog"
	if version >= 100000 {
		walname = "wal"
	}

	query := "SELECT case when pg_is_in_recovery() then '' else (pg_" + walname + "file_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn"
	err = conn.QueryRow(query, backup).Scan(&name, &lsnStr, &replica)
	if err != nil {
		return "", errors.Wrap(err, "QueryFile: start backup failed")
	}
	if replica {
		name, err = WALFileName(lsnStr, path.Join(backupDir, "global", "pg_control"))
		if err != nil {
			return "", err
		}
	}
	return "base_" + name, nil
}

// FormatName grabs the name of the WAL file and returns it in the form of `base_...`.
// If no match is found, returns an empty string and a `NoMatchAvailableError`.
func FormatName(s string) (string, error) {
	re := regexp.MustCompile(`\(([^\)]+)\)`)
	f := re.FindString(s)
	if f == "" {
		return "", errors.Wrap(NoMatchAvailableError{s}, "FormatName:")
	}
	return "base_" + f[6:len(f)-1], nil
}
