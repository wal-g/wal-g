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

func newNoPostgresVersionError() NoPostgresVersionError {
	return NoPostgresVersionError{errors.New("Postgres version not set, cannot determine backup query")}
}

func (err NoPostgresVersionError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnsupportedPostgresVersionError struct {
	error
}

func newUnsupportedPostgresVersionError(version int) UnsupportedPostgresVersionError {
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
	name      string
	oid       walparser.Oid
	tblSpcOid walparser.Oid
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
	mu                sync.Mutex
}

type aoRelPgClassInfo struct {
	oid           uint32
	relNameMd5    string
	relFileNodeID uint32
	relNAtts      int16
	spcNode       uint32
	storage       RelStorageType
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
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// BuildStopBackup formats a query that stops backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStopBackup() (string, error) {
	switch {
	case queryRunner.Version >= 90600:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)", nil
	case queryRunner.Version >= 90000:
		return "SELECT (pg_xlogfile_name_offset(lsn)).file_name," +
			" lpad((pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text " +
			"FROM pg_stop_backup() lsn", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
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
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(queryRunner.buildGetVersion()).Scan(&queryRunner.Version)
	return errors.Wrap(err, "GetVersion: getting Postgres version failed")
}

// Get current LSN of cluster
func (queryRunner *PgQueryRunner) getCurrentLsn() (lsn string, err error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(queryRunner.buildGetCurrentLsn()).Scan(&lsn)
	if err != nil {
		return "", errors.Wrap(err, "GetCurrentLsn: getting current LSN of the cluster failed")
	}
	return lsn, nil
}

func (queryRunner *PgQueryRunner) getSystemIdentifier() (err error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
			"ON info.oid = s.relid " +
			"WHERE relfilenode != 0 " +
			"AND n_tup_ins IS NOT NULL", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// getStatistics queries the relations statistics from database
func (queryRunner *PgQueryRunner) getStatistics(
	dbInfo PgDatabaseInfo) (map[walparser.RelFileNode]PgRelationStat, error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
		relFileNode := walparser.RelFileNode{DBNode: dbInfo.oid,
			RelNode: walparser.Oid(relFileNodeID), SpcNode: walparser.Oid(spcNode)}
		// if tablespace id is zero, use the default database tablespace id
		if relFileNode.SpcNode == walparser.Oid(0) {
			relFileNode.SpcNode = dbInfo.tblSpcOid
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
		return "SELECT oid, datname, dattablespace FROM pg_database WHERE datallowconn", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// getDatabaseInfos fetches a list of all databases in cluster which are allowed to connect
func (queryRunner *PgQueryRunner) getDatabaseInfos() ([]PgDatabaseInfo, error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
		if err := rows.Scan(&dbOid, &dbInfo.name, &dbTblSpcOid); err != nil {
			tracelog.WarningLogger.Printf("GetStatistics:  %v\n", err.Error())
		}
		dbInfo.oid = walparser.Oid(dbOid)
		dbInfo.tblSpcOid = walparser.Oid(dbTblSpcOid)
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
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow("show data_directory").Scan(&dataDir)
	return dataDir, err
}

// GetPhysicalSlotInfo reads information on a physical replication slot
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetPhysicalSlotInfo(slotName string) (PhysicalSlot, error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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

// fetchAOStorageMetadata queries the storage metadata for AO & AOCS tables (GreenplumDB)
func (queryRunner *PgQueryRunner) fetchAOStorageMetadata(dbInfo PgDatabaseInfo) (AoRelFileStorageMap, error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

	tracelog.InfoLogger.Printf("Querying pg_class for %s", dbInfo.name)
	getStatQuery, err := queryRunner.buildAORelPgClassQuery()
	conn := queryRunner.Connection
	if err != nil {
		return nil, errors.Wrap(err, "failed to build the pg_class query")
	}

	rows, err := conn.Query(getStatQuery)
	if err != nil {
		return nil, errors.Wrap(err, "pg_class query failed")
	}

	defer rows.Close()
	relPgClassInfo := make(map[string]aoRelPgClassInfo)
	for rows.Next() {
		var oid uint32
		var relNameMd5 string
		var aoSegTableFqn string
		var relFileNodeID uint32
		var spcNode uint32
		var storage RelStorageType
		var relNAtts int16
		if err := rows.Scan(&oid, &relNameMd5, &aoSegTableFqn, &relFileNodeID, &spcNode, &storage, &relNAtts); err != nil {
			return nil, errors.Wrapf(err, "failed to parse query result")
		}
		row := aoRelPgClassInfo{
			oid:           oid,
			relNameMd5:    relNameMd5,
			relFileNodeID: relFileNodeID,
			relNAtts:      relNAtts,
			spcNode:       spcNode,
			storage:       storage,
		}
		relPgClassInfo[aoSegTableFqn] = row
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	relStorageMap := make(AoRelFileStorageMap)

	for aoSegTableFqn, row := range relPgClassInfo {
		var queryFunc func() (*pgx.Rows, error)
		switch row.storage {
		case AppendOptimized:
			queryFunc = func() (*pgx.Rows, error) {
				query, err := queryRunner.buildAOMetadataQuery(aoSegTableFqn)
				if err != nil {
					return nil, err
				}

				return conn.Query(query)
			}
		case ColumnOriented:
			queryFunc = func() (*pgx.Rows, error) {
				query, err := queryRunner.buildAOCSMetadataQuery()
				if err != nil {
					return nil, err
				}

				return conn.Query(query, row.oid)
			}
		default:
			tracelog.WarningLogger.Printf("Unexpected relation storage type %c for relfilenode %d in database %s",
				row.storage, row.relFileNodeID, dbInfo.name)
			continue
		}

		err = loadStorageMetadata(relStorageMap, dbInfo, queryFunc, aoSegTableFqn, relPgClassInfo)
		if err != nil {
			tracelog.WarningLogger.Printf("failed to fetch the AOCS storage metadata: %v\n", err)
		}
	}

	return relStorageMap, nil
}

func loadStorageMetadata(relStorageMap AoRelFileStorageMap, dbInfo PgDatabaseInfo,
	queryFn func() (*pgx.Rows, error), aoSegTableFqn string, relPgClassInfo map[string]aoRelPgClassInfo) error {
	rows, err := queryFn()
	if err != nil {
		return errors.Wrap(err, "storage metadata query failed")
	}

	defer rows.Close()
	for rows.Next() {
		var segNo int
		var modCount int64
		var eof int64

		if err := rows.Scan(&segNo, &modCount, &eof); err != nil {
			tracelog.WarningLogger.Printf("failed to parse query result: %v\n", err.Error())
		}

		cInfo := relPgClassInfo[aoSegTableFqn]
		relFileLoc := walparser.NewBlockLocation(walparser.Oid(cInfo.spcNode), dbInfo.oid, walparser.Oid(cInfo.relFileNodeID), uint32(segNo))
		// if tablespace id is zero, use the default database tablespace id
		if relFileLoc.RelationFileNode.SpcNode == walparser.Oid(0) {
			relFileLoc.RelationFileNode.SpcNode = dbInfo.tblSpcOid
		}
		relStorageMap[*relFileLoc] = AoRelFileMetadata{
			relNameMd5:  cInfo.relNameMd5,
			storageType: cInfo.storage,
			eof:         eof,
			modCount:    modCount,
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func (queryRunner *PgQueryRunner) readTimeline() (timeline uint32, err error) {
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

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
	queryRunner.mu.Lock()
	defer queryRunner.mu.Unlock()

	ctx := context.Background()
	return queryRunner.Connection.Ping(ctx)
}

func (queryRunner *PgQueryRunner) buildAORelPgClassQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return `
SELECT seg.aooid, md5(seg.aotablefqn), 'pg_aoseg.' || quote_ident(aoseg_c.relname) AS aosegtablefqn,
	seg.relfilenode, seg.reltablespace, seg.relstorage, seg.relnatts 
FROM pg_class aoseg_c
JOIN (
	SELECT pg_ao.relid AS aooid, pg_ao.segrelid, 
			aotables.aotablefqn, aotables.relstorage, 
			aotables.relnatts, aotables.relfilenode, aotables.reltablespace
	FROM pg_appendonly pg_ao
	JOIN (
		SELECT c.oid, quote_ident(n.nspname)|| '.' || quote_ident(c.relname) AS aotablefqn, 
				c.relstorage, c.relnatts, c.relfilenode, c.reltablespace 
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE relstorage IN ( 'ao', 'co' ) AND relpersistence='p'
		) aotables ON pg_ao.relid = aotables.oid
	) seg ON aoseg_c.oid = seg.segrelid;
`, nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

func (queryRunner *PgQueryRunner) buildAOCSMetadataQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return `SELECT aocs.physical_segno as segno, aocs.modcount, aocs.eof " +
			"FROM gp_toolkit.__gp_aocsseg($1::oid) aocs;`, nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

func (queryRunner *PgQueryRunner) buildAOMetadataQuery(aoSegTableFqn string) (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return fmt.Sprintf(`SELECT segno, modcount, eof FROM %s;`, aoSegTableFqn), nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}
