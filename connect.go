package walg

import (
	"fmt"
	"github.com/jackc/pgx"
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
func QueryFile(conn *pgx.Conn, backup string) (string, string) {
	rows, err := conn.Query("SELECT * FROM pg_start_backup($1, true, false)", backup)
	if err != nil {
		panic(err)
	}
	rows.Close()

	var labelfile string
	var spcmapfile string
	err = conn.QueryRow("SELECT labelfile, spcmapfile FROM pg_stop_backup(false)").Scan(&labelfile, &spcmapfile)
	if err != nil {
		panic(err)
	}

	return labelfile, spcmapfile
}

/**
 *  Grabs the name of the WAL file and returns it in the form of `base_...`.
 */
func FormatName(s string) string {
	re := regexp.MustCompile(`\(([^\)]+)\)`)
	f := re.FindString(s)
	return "base_" + f[6:len(f)-1]
}
