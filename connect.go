package walg

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"os"
	"regexp"
)

/**
 *  Connects to postgres using a UNIX socket. Must export PGHOST and
 *  run with `sudo -E -u postgres`.
 */
func Connect() (*pgx.Conn, error) {
	host := os.Getenv("PGHOST")
	if host == "" {
		fmt.Println("Did not set PGHOST.")
		os.Exit(1)
	}
	config := pgx.ConnConfig{
		Host: host,
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

/**
 *  Starts a nonexclusive backup immediately. Finishes backup.
 */
func QueryFile(conn *pgx.Conn, backup string) (string, string, error) {
	var err error
	rows, e := conn.Query("SELECT * FROM pg_start_backup($1, true, false)", backup)
	if e != nil {
		err = errors.Wrap(e, "select query failed")
		return "", "", err
	}
	rows.Close()

	var labelfile string
	var spcmapfile string
	e = conn.QueryRow("SELECT labelfile, spcmapfile FROM pg_stop_backup(false)").Scan(&labelfile, &spcmapfile)
	if e != nil {
		err = errors.Wrap(e, "select query failed")
		return "", "", err
	}

	return labelfile, spcmapfile, err
}

/**
 *  Grabs the name of the WAL file and returns it in the form of `base_...`.
 *  If no match is found, returns an empty string and a NoMatchAvailableError.
 */
func FormatName(s string) (string, error) {
	re := regexp.MustCompile(`\(([^\)]+)\)`)
	f := re.FindString(s)
	if f == "" {
		return "", NoMatchAvailableError{s}
	}
	return "base_" + f[6:len(f)-1], nil
}
