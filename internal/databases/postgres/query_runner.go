package postgres

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
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

type PgDatabaseInfo struct {
	Name      string
	Oid       walparser.Oid
	TblSpcOid walparser.Oid
}

type PgRelationStat struct {
	insertedTuplesCount uint64
	updatedTuplesCount  uint64
	deletedTuplesCount  uint64
}

// PgQueryRunner is implementation for controlling PostgreSQL 9.0+
type PgQueryRunner struct {
	Connection        *pgx.Conn
	Version           int
	SystemIdentifier  *uint64
	stopBackupTimeout time.Duration
	Mu                sync.Mutex
}

// BuildGetVersion formats a query to retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) buildGetVersion() string {
	return "select (current_setting('server_version_num'))::int"
}

// BuildGetCurrentLSN formats a query to get cluster LSN
func (queryRunner *PgQueryRunner) buildGetCurrentLsn() string {
	if queryRunner.Version >= 100000 {
		return "SELECT CASE " +
			"WHEN pg_is_in_recovery() " +
			"THEN pg_last_wal_receive_lsn() " +
			"ELSE pg_current_wal_lsn() " +
			"END"
	}
	return "SELECT CASE " +
		"WHEN pg_is_in_recovery() " +
		"THEN pg_last_xlog_receive_location() " +
		"ELSE pg_current_xlog_location() " +
		"END"
}

// BuildStartBackup formats a query that starts backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStartBackup() (string, error) {
	// TODO: rewrite queries for older versions to remove pg_is_in_recovery()
	// where pg_start_backup() will fail on standby anyway
	switch {
	case queryRunner.Version >= 150000:
		return "SELECT case when pg_is_in_recovery()" +
			" then '' else (pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery()" +
			" FROM pg_backup_start($1, true) lsn", nil
	case queryRunner.Version >= 100000:
		return "SELECT case when pg_is_in_recovery()" +
			" then '' else (pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery()" +
			" FROM pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90600:
		return "SELECT case when pg_is_in_recovery() " +
			"then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery()" +
			" FROM pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90000:
		return "SELECT case when pg_is_in_recovery() " +
			"then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery()" +
			" FROM pg_start_backup($1, true) lsn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// BuildStopBackup formats a query that stops backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStopBackup() (string, error) {
	switch {
	case queryRunner.Version >= 150000:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_backup_stop(false)", nil
	case queryRunner.Version >= 90600:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)", nil
	case queryRunner.Version >= 90000:
		return "SELECT (pg_xlogfile_name_offset(lsn)).file_name," +
			" lpad((pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text " +
			"FROM pg_stop_backup() lsn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// NewPgQueryRunner builds QueryRunner from available connection
func NewPgQueryRunner(conn *pgx.Conn) (*PgQueryRunner, error) {
	timeout, err := getStopBackupTimeoutSetting()
	if err != nil {
		return nil, err
	}

	r := &PgQueryRunner{Connection: conn, stopBackupTimeout: timeout}

	err = r.getVersion()
	if err != nil {
		return nil, err
	}
	err = r.getSystemIdentifier()
	if err != nil {
		tracelog.WarningLogger.Printf("Couldn't get system identifier because of error: '%v'\n", err)
	}

	return r, nil
}

// buildGetSystemIdentifier formats a query that which gathers SystemIdentifier info
// TODO: Unittest
func (queryRunner *PgQueryRunner) buildGetSystemIdentifier() string {
	return "select system_identifier from pg_control_system();"
}

// buildGetParameter formats a query to get a postgresql.conf parameter
// TODO: Unittest
func (queryRunner *PgQueryRunner) buildGetParameter() string {
	return "select setting from pg_settings where name = $1"
}

// buildGetPhysicalSlotInfo formats a query to get info on a Physical Replication Slot
// TODO: Unittest
func (queryRunner *PgQueryRunner) buildGetPhysicalSlotInfo() string {
	return "select active, restart_lsn from pg_replication_slots where slot_name = $1"
}

// Retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) getVersion() (err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(queryRunner.buildGetVersion()).Scan(&queryRunner.Version)
	return errors.Wrap(err, "GetVersion: getting Postgres version failed")
}

// Get current LSN of cluster
func (queryRunner *PgQueryRunner) getCurrentLsn() (lsn string, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(queryRunner.buildGetCurrentLsn()).Scan(&lsn)
	if err != nil {
		return "", errors.Wrap(err, "GetCurrentLsn: getting current LSN of the cluster failed")
	}
	return lsn, nil
}

func (queryRunner *PgQueryRunner) getSystemIdentifier() (err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	if queryRunner.Version < 90600 {
		tracelog.WarningLogger.Println("GetSystemIdentifier: Unable to get system identifier")
		return nil
	}
	conn := queryRunner.Connection
	err = conn.QueryRow(queryRunner.buildGetSystemIdentifier()).Scan(&queryRunner.SystemIdentifier)
	return errors.Wrap(err, "System Identifier: getting identifier of DB failed")
}

// StartBackup informs the database that we are starting copy of cluster contents
func (queryRunner *PgQueryRunner) startBackup(backup string) (backupName string,
	lsnString string, inRecovery bool, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Calling pg_start_backup()")
	startBackupQuery, err := queryRunner.BuildStartBackup()
	conn := queryRunner.Connection
	if err != nil {
		return "", "", false, errors.Wrap(err, "QueryRunner StartBackup: Building start backup query failed")
	}

	if err = conn.QueryRow(startBackupQuery, backup).Scan(&backupName, &lsnString, &inRecovery); err != nil {
		return "", "", false, errors.Wrap(err, "QueryRunner StartBackup: pg_start_backup() failed")
	}

	return backupName, lsnString, inRecovery, nil
}

// StopBackup informs the database that copy is over
func (queryRunner *PgQueryRunner) stopBackup() (label string, offsetMap string, lsnStr string, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Calling pg_stop_backup()")
	conn := queryRunner.Connection

	tx, err := conn.Begin()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: transaction begin failed")
	}
	defer func() {
		// ignore the possible error, it's ok
		_ = tx.Rollback()
	}()

	_, err = tx.Exec(fmt.Sprintf("SET statement_timeout=%d;", queryRunner.stopBackupTimeout.Milliseconds()))
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

// BuildStatisticsQuery formats a query that fetch relations statistics from database
func (queryRunner *PgQueryRunner) BuildStatisticsQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return "SELECT info.relfilenode, info.reltablespace, s.n_tup_ins, s.n_tup_upd, s.n_tup_del " +
			"FROM pg_class info " +
			"LEFT OUTER JOIN pg_stat_all_tables s " +
			"ON info.Oid = s.relid " +
			"WHERE relfilenode != 0 " +
			"AND n_tup_ins IS NOT NULL", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// getStatistics queries the relations statistics from database
func (queryRunner *PgQueryRunner) getStatistics(
	dbInfo PgDatabaseInfo) (map[walparser.RelFileNode]PgRelationStat, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Querying pg_stat_all_tables")
	getStatQuery, err := queryRunner.BuildStatisticsQuery()
	conn := queryRunner.Connection
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetStatistics: Building get statistics query failed")
	}

	rows, err := conn.Query(getStatQuery)
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetStatistics: pg_stat_all_tables query failed")
	}

	defer rows.Close()
	relationsStats := make(map[walparser.RelFileNode]PgRelationStat)
	for rows.Next() {
		var relationStat PgRelationStat
		var relFileNodeID uint32
		var spcNode uint32
		if err := rows.Scan(&relFileNodeID, &spcNode, &relationStat.insertedTuplesCount, &relationStat.updatedTuplesCount,
			&relationStat.deletedTuplesCount); err != nil {
			tracelog.WarningLogger.Printf("GetStatistics:  %v\n", err.Error())
		}
		relFileNode := walparser.RelFileNode{DBNode: dbInfo.Oid,
			RelNode: walparser.Oid(relFileNodeID), SpcNode: walparser.Oid(spcNode)}
		// if tablespace id is zero, use the default database tablespace id
		if relFileNode.SpcNode == walparser.Oid(0) {
			relFileNode.SpcNode = dbInfo.TblSpcOid
		}
		relationsStats[relFileNode] = relationStat
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return relationsStats, nil
}

// BuildGetDatabasesQuery formats a query to get all databases in cluster which are allowed to connect
func (queryRunner *PgQueryRunner) BuildGetDatabasesQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return "SELECT Oid, datname, dattablespace FROM pg_database WHERE datallowconn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// GetDatabaseInfos fetches a list of all databases in cluster which are allowed to connect
func (queryRunner *PgQueryRunner) GetDatabaseInfos() ([]PgDatabaseInfo, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Querying pg_database")
	getDBInfoQuery, err := queryRunner.BuildGetDatabasesQuery()
	conn := queryRunner.Connection
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetDatabases: Building db names query failed")
	}

	rows, err := conn.Query(getDBInfoQuery)
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetDatabases: pg_database query failed")
	}

	defer rows.Close()
	databases := make([]PgDatabaseInfo, 0)
	for rows.Next() {
		dbInfo := PgDatabaseInfo{}
		var dbOid uint32
		var dbTblSpcOid uint32
		if err := rows.Scan(&dbOid, &dbInfo.Name, &dbTblSpcOid); err != nil {
			tracelog.WarningLogger.Printf("GetStatistics:  %v\n", err.Error())
		}
		dbInfo.Oid = walparser.Oid(dbOid)
		dbInfo.TblSpcOid = walparser.Oid(dbTblSpcOid)
		databases = append(databases, dbInfo)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return databases, nil
}

// GetParameter reads a Postgres setting
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetParameter(parameterName string) (string, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var value string
	conn := queryRunner.Connection
	err := conn.QueryRow(queryRunner.buildGetParameter(), parameterName).Scan(&value)
	return value, err
}

// GetWalSegmentBytes reads the wals segment size (in bytes) and converts it to uint64
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetWalSegmentBytes() (segBlocks uint64, err error) {
	strValue, err := queryRunner.GetParameter("wal_segment_size")
	if err != nil {
		return 0, err
	}
	segBlocks, err = strconv.ParseUint(strValue, 10, 64)
	if err != nil {
		return 0, err
	}
	if queryRunner.Version < 110000 {
		// For PG 10 and below, wal_segment_size is in 8k blocks
		segBlocks *= 8192
	}
	return
}

// GetDataDir reads the wals segment size (in bytes) and converts it to uint64
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetDataDir() (dataDir string, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow("show data_directory").Scan(&dataDir)
	return dataDir, err
}

// GetPhysicalSlotInfo reads information on a physical replication slot
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetPhysicalSlotInfo(slotName string) (PhysicalSlot, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var active bool
	var restartLSN string

	conn := queryRunner.Connection
	err := conn.QueryRow(queryRunner.buildGetPhysicalSlotInfo(), slotName).Scan(&active, &restartLSN)
	if err == pgx.ErrNoRows {
		// slot does not exist.
		return PhysicalSlot{Name: slotName}, nil
	} else if err != nil {
		return PhysicalSlot{Name: slotName}, err
	}
	return NewPhysicalSlot(slotName, true, active, restartLSN)
}

// tablespace map does not exist in < 9.6
// TODO: Unittest
func (queryRunner *PgQueryRunner) IsTablespaceMapExists() bool {
	return queryRunner.Version >= 90600
}

func (queryRunner *PgQueryRunner) readTimeline() (timeline uint32, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	var bytesPerWalSegment uint32

	err = conn.QueryRow("select timeline_id, bytes_per_wal_segment "+
		"from pg_control_checkpoint(), pg_control_init()").Scan(&timeline, &bytesPerWalSegment)
	if err == nil && uint64(bytesPerWalSegment) != WalSegmentSize {
		return 0, newBytesPerWalSegmentError()
	}
	return
}

func (queryRunner *PgQueryRunner) Ping() error {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	ctx := context.Background()
	return queryRunner.Connection.Ping(ctx)
}

func (queryRunner *PgQueryRunner) ForEachDatabase(
	function func(runner *PgQueryRunner, database PgDatabaseInfo) error) error {
	databases, err := queryRunner.GetDatabaseInfos()
	if err != nil {
		return errors.Wrap(err, "Failed to get db names.")
	}

	for _, db := range databases {
		dbName := db.Name
		databaseOption := func(c *pgx.ConnConfig) error {
			c.Database = dbName
			return nil
		}
		dbConn, err := Connect(databaseOption)

		if err != nil {
			tracelog.WarningLogger.Printf("Failed to collect statistics for database: %s\n'%v'\n", db.Name, err)
			continue
		}
		runner, err := NewPgQueryRunner(dbConn)
		if err != nil {
			return errors.Wrap(err, "Failed to build query runner.")
		}

		err = function(runner, db)
		if err != nil {
			return err
		}

		err = dbConn.Close()
		tracelog.WarningLogger.PrintOnError(err)
	}
	return nil
}
