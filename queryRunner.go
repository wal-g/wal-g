package walg

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// The interface for controlling database during backup
type QueryRunner interface {
	// This call should informa the database that we are going to copy cluster's contents
	// Should fail if backup is currently impossible
	StartBackup(backup string) (string, string, bool, error)
	// Inform database that contents are copied, get information on backup
	StopBackup() (string, string, string, error)
}

// Impelemntation for controlling PostgreSQL 9.0+
type PgQueryRunner struct {
	connection *pgx.Conn
	Version    int
}

// Formats a query to retrieve PostgreSQL numeric version
func (qb *PgQueryRunner) BuildGetVersion() string {
	return "select (current_setting('server_version_num'))::int"
}

// Format a query that starts backup accroding to server features and version
func (qb *PgQueryRunner) BuildStartBackup() (string, error) {
	// TODO: rewrite queries for older versions to remove pg_is_in_recovery()
	// where pg_start_backup() will fail on standby anyway
	switch {
	case qb.Version >= 100000:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn", nil
	case qb.Version >= 90600:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn", nil
	case qb.Version >= 90000:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true) lsn", nil
	case qb.Version == 0:
		return "", errors.New("Postgres version not set, cannot determing start backup query")
	default:
		return "", errors.New("Could not determine start backup query for version " + fmt.Sprintf("%d", qb.Version))
	}
}

// Format a query that stops backup accroding to server features and version
func (qb *PgQueryRunner) BuildStopBackup() (string, error) {
	switch {
	case qb.Version >= 90600:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)", nil
	case qb.Version >= 90000:
		return "SELECT (pg_xlogfile_name_offset(lsn)).file_name, lpad((pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text FROM pg_stop_backup() lsn", nil
	case qb.Version == 0:
		return "", errors.New("Postgres version not set, cannot determing stop backup query")
	default:
		return "", errors.New("Could not determine stop backup query for version " + fmt.Sprintf("%d", qb.Version))
	}
}

// Build QueryRunner from available connection
func NewPgQueryRunner(conn *pgx.Conn) (*PgQueryRunner, error) {
	r := &PgQueryRunner{connection: conn}

	err := r.getVersion()
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Retrive PostgreSQL numeric version
func (queryRunner *PgQueryRunner) getVersion() (err error) {
	conn := queryRunner.connection
	err = conn.QueryRow(queryRunner.BuildGetVersion()).Scan(&queryRunner.Version)
	if err != nil {
		return errors.Wrap(err, "GetVersion: getting Postgres version failed")
	}
	return nil
}

// Inform the database that we are starting copy of cluster contents
func (queryRunner *PgQueryRunner) StartBackup(backup string) (backupName string, lsnString string, inRecovery bool, err error) {
	startBackupQuery, err := queryRunner.BuildStartBackup()
	conn := queryRunner.connection
	if err != nil {
		return "", "", false, errors.Wrap(err, "QueryRunner StartBackup: Building start backup query failed")
	}

	if err = conn.QueryRow(startBackupQuery, backup).Scan(&backupName, &lsnString, &inRecovery); err != nil {
		return "", "", false, errors.Wrap(err, "QueryRunner StartBackup: pg_start_backup() failed")
	}

	return backupName, lsnString, inRecovery, nil
}

// Inform the database that copy is over
func (queryRunner *PgQueryRunner) StopBackup() (label string, offsetMap string, lsnStr string, err error) {
	conn := queryRunner.connection

	tx, err := conn.Begin()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: transaction begin failed")
	}
	defer tx.Rollback()

	_, err = tx.Exec("SET statement_timeout=0;")
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: failed setting statement timeout in transaction")
	}

	stopBackupQuery, err := queryRunner.BuildStopBackup()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: Building stop backup query failed")
	}

	err = tx.QueryRow(stopBackupQuery).Scan(&label, &offsetMap, &lsnStr)
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: stop backup failed")
	}

	err = tx.Commit()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: commit failed")
	}

	return label, offsetMap, lsnStr, nil
}
