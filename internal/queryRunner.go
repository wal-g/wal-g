package internal

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/tinsane/tracelog"
)

type NoPostgresVersionError struct {
	error
}

func NewNoPostgresVersionError() NoPostgresVersionError {
	return NoPostgresVersionError{errors.New("Postgres version not set, cannot determine backup query")}
}

func (err NoPostgresVersionError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnsupportedPostgresVersionError struct {
	error
}

func NewUnsupportedPostgresVersionError(version int) UnsupportedPostgresVersionError {
	return UnsupportedPostgresVersionError{errors.Errorf("Could not determine backup query for version %d", version)}
}

func (err UnsupportedPostgresVersionError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// The QueryRunner interface for controlling database during backup
type QueryRunner interface {
	// This call should inform the database that we are going to copy cluster's contents
	// Should fail if backup is currently impossible
	StartBackup(backup string) (string, string, bool, error)
	// Inform database that contents are copied, get information on backup
	StopBackup() (string, string, string, error)
}

// PgQueryRunner is implementation for controlling PostgreSQL 9.0+
type PgQueryRunner struct {
	connection *pgx.Conn
	Version    int
}

// BuildGetVersion formats a query to retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) BuildGetVersion() string {
	return "select (current_setting('server_version_num'))::int"
}

// BuildStartBackup formats a query that starts backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStartBackup() (string, error) {
	// TODO: rewrite queries for older versions to remove pg_is_in_recovery()
	// where pg_start_backup() will fail on standby anyway
	switch {
	case queryRunner.Version >= 100000:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90600:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90000:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true) lsn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// BuildStopBackup formats a query that stops backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStopBackup() (string, error) {
	switch {
	case queryRunner.Version >= 90600:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)", nil
	case queryRunner.Version >= 90000:
		return "SELECT (pg_xlogfile_name_offset(lsn)).file_name, lpad((pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text FROM pg_stop_backup() lsn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// NewPgQueryRunner builds QueryRunner from available connection
func NewPgQueryRunner(conn *pgx.Conn) (*PgQueryRunner, error) {
	r := &PgQueryRunner{connection: conn}

	err := r.getVersion()
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) getVersion() (err error) {
	conn := queryRunner.connection
	err = conn.QueryRow(queryRunner.BuildGetVersion()).Scan(&queryRunner.Version)
	return errors.Wrap(err, "GetVersion: getting Postgres version failed")
}

// StartBackup informs the database that we are starting copy of cluster contents
func (queryRunner *PgQueryRunner) StartBackup(backup string) (backupName string, lsnString string, inRecovery bool, dataDir string, err error) {
	tracelog.InfoLogger.Println("Calling pg_start_backup()")
	startBackupQuery, err := queryRunner.BuildStartBackup()
	conn := queryRunner.connection
	if err != nil {
		return "", "", false, "", errors.Wrap(err, "QueryRunner StartBackup: Building start backup query failed")
	}

	if err = conn.QueryRow(startBackupQuery, backup).Scan(&backupName, &lsnString, &inRecovery); err != nil {
		return "", "", false, "", errors.Wrap(err, "QueryRunner StartBackup: pg_start_backup() failed")
	}

	if err = conn.QueryRow("show data_directory").Scan(&dataDir); err != nil {
		return "", "", false, "", errors.Wrap(err, "QueryRunner StartBackup: show data_directory failed")
	}

	return backupName, lsnString, inRecovery, dataDir, nil
}

// StopBackup informs the database that copy is over
func (queryRunner *PgQueryRunner) StopBackup() (label string, offsetMap string, lsnStr string, err error) {
	tracelog.InfoLogger.Println("Calling pg_stop_backup()")
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
