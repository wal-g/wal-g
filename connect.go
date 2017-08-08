package walg

import (
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"os"
	"regexp"
)

// Connect establishes a connection to postgres using
// a UNIX socket. Must export PGHOST and run with `sudo -E -u postgres`.
// If PGHOST is not set or if the connection fails, an error is returned
// and the connection is `<nil>`.
func Connect() (*pgx.Conn, error) {
	host := os.Getenv("PGHOST")
	if host == "" {
		return nil, errors.New("Connect: did not set PGHOST")
	}

	config := pgx.ConnConfig{
		Host: host,
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return nil, errors.Wrap(err, "Connect: postgres connection failed")
	}

	return conn, nil
}

// Starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty strings and an error if backup
// fails.
func QueryFile(conn *pgx.Conn, backup string) (string, string, error) {
	rows, err := conn.Query("SELECT * FROM pg_start_backup($1, true, false)", backup)
	if err != nil {
		return "", "", errors.Wrap(err, "QueryFile: start backup failed")
	}
	rows.Close()

	var labelfile string
	var spcmapfile string
	err = conn.QueryRow("SELECT labelfile, spcmapfile FROM pg_stop_backup(false)").Scan(&labelfile, &spcmapfile)
	if err != nil {
		return "", "", errors.Wrap(err, "QueryFile: stop backup failed")
	}

	return labelfile, spcmapfile, nil
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
