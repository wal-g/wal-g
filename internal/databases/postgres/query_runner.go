package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/utility"
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
	StartBackup(ctx context.Context, backup string) (string, string, bool, error)
	// Inform database that contents are copied, get information on backup
	StopBackup(ctx context.Context) (string, string, string, error)
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
			"WHEN pg_catalog.pg_is_in_recovery() " +
			"THEN pg_catalog.pg_last_wal_receive_lsn() " +
			"ELSE pg_catalog.pg_current_wal_lsn() " +
			"END"
	}
	return "SELECT CASE " +
		"WHEN pg_catalog.pg_is_in_recovery() " +
		"THEN pg_catalog.pg_last_xlog_receive_location() " +
		"ELSE pg_catalog.pg_current_xlog_location() " +
		"END"
}

// BuildStartBackup formats a query that starts backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStartBackup() (string, error) {
	// TODO: rewrite queries for older versions to remove pg_is_in_recovery()
	// where pg_start_backup() will fail on standby anyway
	switch {
	case queryRunner.Version >= 150000:
		return "SELECT case when pg_catalog.pg_is_in_recovery()" +
			" then '' else (pg_catalog.pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_catalog.pg_is_in_recovery()" +
			" FROM pg_catalog.pg_backup_start($1, true) lsn", nil
	case queryRunner.Version >= 100000:
		return "SELECT case when pg_catalog.pg_is_in_recovery()" +
			" then '' else (pg_catalog.pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_catalog.pg_is_in_recovery()" +
			" FROM pg_catalog.pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90600:
		return "SELECT case when pg_catalog.pg_is_in_recovery() " +
			"then '' else (pg_catalog.pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_catalog.pg_is_in_recovery()" +
			" FROM pg_catalog.pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90000:
		return "SELECT case when pg_catalog.pg_is_in_recovery() " +
			"then '' else (pg_catalog.pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_catalog.pg_is_in_recovery()" +
			" FROM pg_catalog.pg_start_backup($1, true) lsn", nil
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
		return "SELECT labelfile, spcmapfile, lsn FROM pg_catalog.pg_backup_stop()", nil
	case queryRunner.Version >= 90600:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_catalog.pg_stop_backup(false)", nil
	case queryRunner.Version >= 90000:
		return "SELECT (pg_catalog.pg_xlogfile_name_offset(lsn)).file_name," +
			" lpad((pg_catalog.pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text " +
			"FROM pg_catalog.pg_stop_backup() lsn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// NewPgQueryRunner builds QueryRunner from available connection
func NewPgQueryRunner(ctx context.Context, conn *pgx.Conn) (*PgQueryRunner, error) {
	timeout, err := getStopBackupTimeoutSetting()
	if err != nil {
		return nil, err
	}

	r := &PgQueryRunner{Connection: conn, stopBackupTimeout: timeout}

	err = r.getVersion(ctx)
	if err != nil {
		return nil, err
	}
	err = r.getSystemIdentifier(ctx)
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
	return "select active, restart_lsn from pg_catalog.pg_replication_slots where slot_name = $1"
}

// Retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) getVersion(ctx context.Context) (err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(ctx, queryRunner.buildGetVersion()).Scan(&queryRunner.Version)
	return errors.Wrap(err, "GetVersion: getting Postgres version failed")
}

// Get current LSN of cluster
func (queryRunner *PgQueryRunner) getCurrentLsn(ctx context.Context) (lsn string, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(ctx, queryRunner.buildGetCurrentLsn()).Scan(&lsn)
	if err != nil {
		return "", errors.Wrap(err, "GetCurrentLsn: getting current LSN of the cluster failed")
	}
	return lsn, nil
}

func (queryRunner *PgQueryRunner) getSystemIdentifier(ctx context.Context) (err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	if queryRunner.Version < 90600 {
		tracelog.WarningLogger.Println("GetSystemIdentifier: Unable to get system identifier")
		return nil
	}
	conn := queryRunner.Connection
	var systemIdentifier int64
	err = conn.QueryRow(ctx, queryRunner.buildGetSystemIdentifier()).Scan(&systemIdentifier)
	if err == nil {
		// Intentionally scan into int64 and then convert to uint64 so negative
		// int64 values are reinterpreted via two's-complement wraparound. This
		// preserves the system identifier representation expected by PostgreSQL
		// tooling; do not replace this with a direct uint64 scan.
		converted := uint64(systemIdentifier)
		queryRunner.SystemIdentifier = &converted
	}
	return errors.Wrap(err, "System Identifier: getting identifier of DB failed")
}

// StartBackup informs the database that we are starting copy of cluster contents
func (queryRunner *PgQueryRunner) StartBackup(ctx context.Context, backup string) (backupName string,
	lsnString string, inRecovery bool, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Calling pg_start_backup()")
	startBackupQuery, err := queryRunner.BuildStartBackup()
	conn := queryRunner.Connection
	if err != nil {
		return "", "", false, errors.Wrap(err, "QueryRunner StartBackup: Building start backup query failed")
	}

	if err = conn.QueryRow(ctx, startBackupQuery, backup).Scan(&backupName, &lsnString, &inRecovery); err != nil {
		return "", "", false, errors.Wrap(err, "QueryRunner StartBackup: pg_start_backup() failed")
	}

	return backupName, lsnString, inRecovery, nil
}

// StopBackup informs the database that copy is over
func (queryRunner *PgQueryRunner) StopBackup(ctx context.Context) (label string, offsetMap string, lsnStr string, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Calling pg_stop_backup()")

	// Cleanup must not be cancellable: a signal mid-backup must not strand Postgres in backup mode.
	// Detach cancellation while preserving values/tracing; statement_timeout bounds the server side.
	cleanupCtx := context.WithoutCancel(ctx)

	//during long backups connection might break, so we check if connection is still alive
	errPing := queryRunner.Connection.Ping(cleanupCtx)
	if errPing != nil {
		queryRunner.Connection, err = Connect(cleanupCtx)
		if err != nil {
			return "", "", "", fmt.Errorf("QueryRunner StopBackup: connection is dead: %w %w", errPing, err)
		}
	}

	conn := queryRunner.Connection

	tx, err := conn.Begin(cleanupCtx)
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: transaction begin failed")
	}
	defer func() {
		// ignore the possible error, it's ok
		_ = tx.Rollback(cleanupCtx)
	}()

	_, err = tx.Exec(cleanupCtx, fmt.Sprintf("SET statement_timeout=%d;", queryRunner.stopBackupTimeout.Milliseconds()))
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: failed setting statement timeout in transaction")
	}

	stopBackupQuery, err := queryRunner.BuildStopBackup()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: Building stop backup query failed")
	}

	err = tx.QueryRow(cleanupCtx, stopBackupQuery).Scan(&label, &offsetMap, &lsnStr)
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: stop backup failed")
	}

	err = tx.Commit(cleanupCtx)
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
			"FROM pg_catalog.pg_class info " +
			"LEFT OUTER JOIN pg_catalog.pg_stat_all_tables s " +
			"ON info.Oid OPERATOR(pg_catalog.=) s.relid " +
			"WHERE relfilenode OPERATOR(pg_catalog.!=) 0 " +
			"AND n_tup_ins IS NOT NULL", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// getStatistics queries the relations statistics from database
func (queryRunner *PgQueryRunner) getStatistics(ctx context.Context,
	dbInfo PgDatabaseInfo) (map[walparser.RelFileNode]PgRelationStat, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Querying pg_stat_all_tables")
	getStatQuery, err := queryRunner.BuildStatisticsQuery()
	conn := queryRunner.Connection
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetStatistics: Building get statistics query failed")
	}

	rows, err := conn.Query(ctx, getStatQuery)
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
		return "SELECT Oid, datname, dattablespace FROM pg_catalog.pg_database WHERE datallowconn", nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// GetDatabaseInfos fetches a list of all databases in cluster which are allowed to connect
func (queryRunner *PgQueryRunner) GetDatabaseInfos(ctx context.Context) ([]PgDatabaseInfo, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Println("Querying pg_database")
	getDBInfoQuery, err := queryRunner.BuildGetDatabasesQuery()
	conn := queryRunner.Connection
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetDatabases: Building db names query failed")
	}

	rows, err := conn.Query(ctx, getDBInfoQuery)
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
func (queryRunner *PgQueryRunner) GetParameter(ctx context.Context, parameterName string) (string, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var value string
	conn := queryRunner.Connection
	err := conn.QueryRow(ctx, queryRunner.buildGetParameter(), parameterName).Scan(&value)
	return value, err
}

// GetWalSegmentBytes reads the wals segment size (in bytes) and converts it to uint64
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetWalSegmentBytes(ctx context.Context) (segBlocks uint64, err error) {
	strValue, err := queryRunner.GetParameter(ctx, "wal_segment_size")
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
func (queryRunner *PgQueryRunner) GetDataDir(ctx context.Context) (dataDir string, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	err = conn.QueryRow(ctx, "show data_directory").Scan(&dataDir)
	return dataDir, err
}

// GetPhysicalSlotInfo reads information on a physical replication slot
// TODO: Unittest
func (queryRunner *PgQueryRunner) GetPhysicalSlotInfo(ctx context.Context, slotName string) (PhysicalSlot, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var active bool
	var restartLSN string

	conn := queryRunner.Connection
	err := conn.QueryRow(ctx, queryRunner.buildGetPhysicalSlotInfo(), slotName).Scan(&active, &restartLSN)
	if err == pgx.ErrNoRows {
		// slot does not exist.
		return PhysicalSlot{Name: slotName}, nil
	} else if err != nil {
		return PhysicalSlot{Name: slotName}, err
	}
	return NewPhysicalSlot(slotName, true, active, restartLSN)
}

// tablespace map does not exist in < 9.6
func (queryRunner *PgQueryRunner) IsTablespaceMapExists() bool {
	return queryRunner.Version >= 90600
}

func (queryRunner *PgQueryRunner) ReadTimeline(ctx context.Context) (timeline uint32, err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	var bytesPerWalSegment uint32

	if queryRunner.Version >= 90600 {
		err = conn.QueryRow(ctx, "select timeline_id, bytes_per_wal_segment "+
			"from pg_catalog.pg_control_checkpoint(), pg_catalog.pg_control_init()").Scan(&timeline, &bytesPerWalSegment)
		if err == nil && uint64(bytesPerWalSegment) != WalSegmentSize {
			return 0, newBytesPerWalSegmentError()
		}
	} else {
		var hex string
		err = conn.QueryRow(ctx, "SELECT SUBSTR(pg_catalog.pg_xlogfile_name(pg_catalog.pg_current_xlog_insert_location()), "+
			"1, 8) AS timeline").Scan(&hex)
		if err != nil {
			return
		}
		var time64 uint64
		time64, err = strconv.ParseUint(hex, 16, 64)
		if err != nil {
			return
		}
		timeline = uint32(time64)
	}
	return
}

func (queryRunner *PgQueryRunner) Ping() error {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	ctx := context.Background()
	return queryRunner.Connection.Ping(ctx)
}

func (queryRunner *PgQueryRunner) ForEachDatabase(ctx context.Context,
	function func(runner *PgQueryRunner, db PgDatabaseInfo) error) error {
	databases, err := queryRunner.GetDatabaseInfos(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get db names.")
	}

	for _, db := range databases {
		err := queryRunner.executeForDatabase(ctx, function, db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (queryRunner *PgQueryRunner) executeForDatabase(ctx context.Context,
	function func(runner *PgQueryRunner, db PgDatabaseInfo) error, db PgDatabaseInfo) error {
	dbName := db.Name
	databaseOption := func(c *pgx.ConnConfig) error {
		c.Database = dbName
		return nil
	}
	dbConn, err := Connect(ctx, databaseOption)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to connect to database: %s\n'%v'\n", db.Name, err)
		return nil
	}
	defer utility.LoggedCloseContext(dbConn, "")

	runner, err := NewPgQueryRunner(ctx, dbConn)
	if err != nil {
		return errors.Wrap(err, "Failed to build query runner")
	}

	return function(runner, db)
}

func (queryRunner *PgQueryRunner) TryGetLock(ctx context.Context) (err error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	var lockFree bool
	err = conn.QueryRow(ctx, "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtext('pg_backup'))").Scan(&lockFree)
	if err != nil {
		return err
	}

	if !lockFree {
		return errors.New("Lock is already taken by other process")
	}
	return nil
}

func (queryRunner *PgQueryRunner) GetLockingPID(ctx context.Context) (int, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	conn := queryRunner.Connection
	var pid int
	err := conn.QueryRow(ctx, "SELECT pid FROM pg_catalog.pg_locks WHERE locktype='advisory' AND objid OPERATOR(pg_catalog.=) pg_catalog.hashtext('pg_backup')").Scan(&pid)
	if err != nil {
		return 0, err
	}

	return pid, nil
}

// builds query to list tables.
//
// Returns:
// - string: SQL query
// - error: An error if faces problems, otherwise nil.
func (queryRunner *PgQueryRunner) buildGetTablesQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		// Only invoke pg_relation_filepath for mapped catalogs (relfilenode=0)
		// Over millions of relations exhausts backend RELOID syscache, OOMing query
		// Path only used by processTables when relfilenode=0
		query := "SELECT c.relfilenode, c.oid, " +
			"CASE WHEN c.relfilenode OPERATOR(pg_catalog.=) 0 THEN pg_catalog.pg_relation_filepath(c.oid) END, " +
			"c.relname, pg_namespace.nspname, c.relkind, parent.relname AS parent_name " +
			"FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace ON c.relnamespace OPERATOR(pg_catalog.=) pg_namespace.oid " +
			"LEFT JOIN pg_catalog.pg_inherits i ON c.oid OPERATOR(pg_catalog.=) i.inhrelid LEFT JOIN ppg_catalog.g_class parent ON i.inhparent OPERATOR(pg_catalog.=) parent.oid;"
		return query, nil
	case queryRunner.Version == 0:
		return "", NewNoPostgresVersionError()
	default:
		return "", NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

func (queryRunner *PgQueryRunner) getTables(ctx context.Context) (map[string]TableInfo, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tables := make(map[string]TableInfo, 0)
	parentTables := make([]string, 0)

	getTablesQuery, err := queryRunner.buildGetTablesQuery()
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetTables: Building query failed")
	}

	err = queryRunner.processTables(ctx, queryRunner.Connection, getTablesQuery,
		func(relFileNode, oid uint32, tableName, namespaceName, parentTableName string) {
			tracelog.DebugLogger.Printf("adding %s as %d with filenode %d", tableName, oid, relFileNode)
			parent := fmt.Sprintf("%s.%s", namespaceName, parentTableName)
			child := fmt.Sprintf("%s.%s", namespaceName, tableName)

			entry, ok := tables[child]
			if !ok {
				tables[child] = TableInfo{Oid: oid, Relfilenode: relFileNode, SubTables: map[string]TableInfo{}}
			} else {
				entry.Oid = oid
				entry.Relfilenode = relFileNode
				tables[child] = entry
			}

			if parentTableName == "" {
				parentTables = append(parentTables, child)
				return
			}

			_, ok = tables[parent]
			if !ok {
				tables[parent] = TableInfo{Oid: oid, Relfilenode: relFileNode, SubTables: map[string]TableInfo{child: {}}}
			} else {
				tables[parent].SubTables[child] = TableInfo{}
			}
		})
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetTables: getting regular tables failed")
	}

	formatedTables := make(map[string]TableInfo)
	stack := make([]string, 0)

	for _, val := range parentTables {
		formatedTables[val] = TableInfo{Oid: tables[val].Oid, Relfilenode: tables[val].Relfilenode, SubTables: map[string]TableInfo{}}
		stack = append(stack, val)
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if tables[cur].SubTables == nil {
				continue
			}

			for k := range tables[cur].SubTables {
				formatedTables[val].SubTables[k] = TableInfo{Oid: tables[k].Oid, Relfilenode: tables[k].Relfilenode}
				stack = append(stack, k)
			}
		}
	}

	return formatedTables, nil
}

func (queryRunner *PgQueryRunner) processTables(ctx context.Context, conn *pgx.Conn,
	getTablesQuery string, process func(relFileNode, oid uint32, tableName, namespaceName, parentTableName string)) error {
	rows, err := conn.Query(ctx, getTablesQuery)
	if err != nil {
		tracelog.WarningLogger.Printf("GetTables:  %v\n", err.Error())
		return errors.Wrap(err, "QueryRunner GetTables: Query failed")
	}
	defer rows.Close()

	for rows.Next() {
		var relFileNode uint32
		var oid uint32
		var tableName string
		var namespaceName string
		var path pgtype.Text
		var relKind rune
		var parentTableName pgtype.Text
		if err := rows.Scan(&relFileNode, &oid, &path, &tableName, &namespaceName, &relKind, &parentTableName); err != nil {
			tracelog.WarningLogger.Printf("GetTables:  %v\n", err.Error())
			continue
		}

		if relKind == 'p' {
			// Although partitioned tables have relfilenode=0 (no physical storage) and theoretically
			// don't need to be added to the tables map, we still process them here.
			// This is because we need the parent partitioned table information to locate and process
			// all its child partition tables later in DatabasesByNames.ResolveRegexp function.
			process(relFileNode, oid, tableName, namespaceName, parentTableName.String)
			continue
		}

		// If relFileNode is 0, we need to check the actual storage situation
		if relFileNode == 0 {
			// Case 1: Empty path indicates a relation with no physical storage
			// This happens for:
			// partitioned indexes, views, foreign tables
			if path.String == "" {
				tracelog.DebugLogger.Printf("Skipping relation '%s.%s' (relkind=%c) due to no physical storage", namespaceName, tableName, relKind)
				continue
			}

			// Case 2: Non-empty path with relfilenode=0 indicates a mapped catalog
			// This happens for:
			// pg_class itself and other critical system catalogs, shared catalogs
			// These tables use a separate mapping file to track their actual file locations
			parts := strings.Split(path.String, "/")
			chis, err := strconv.ParseUint(parts[len(parts)-1], 10, 32)
			if err != nil {
				tracelog.DebugLogger.Printf("Failed to get relfilenode for relation %s: %v\n", tableName, err)
				continue
			}
			relFileNode = uint32(chis)
		}
		process(relFileNode, oid, tableName, namespaceName, parentTableName.String)
	}

	return nil
}

// GetDataChecksums checks if data checksums are enabled
func (queryRunner *PgQueryRunner) GetDataChecksums(ctx context.Context) (string, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var dataChecksums string
	conn := queryRunner.Connection
	err := conn.QueryRow(ctx, "SHOW data_checksums").Scan(&dataChecksums)
	if err != nil {
		return "", errors.Wrap(err, "GetDataChecksums: failed to check data_checksums")
	}

	return dataChecksums, nil
}

// GetArchiveMode retrieves the current archive_mode setting.
func (queryRunner *PgQueryRunner) GetArchiveMode(ctx context.Context) (string, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var archiveMode string
	err := queryRunner.Connection.QueryRow(ctx, "SHOW archive_mode").Scan(&archiveMode)
	if err != nil {
		return "", errors.Wrap(err, "GetArchiveMode: failed to retrieve archive_mode")
	}
	return archiveMode, nil
}

// GetArchiveCommand retrieves the current archive_command setting.
func (queryRunner *PgQueryRunner) GetArchiveCommand(ctx context.Context) (string, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var archiveCommand string
	err := queryRunner.Connection.QueryRow(ctx, "SHOW archive_command").Scan(&archiveCommand)
	if err != nil {
		return "", errors.Wrap(err, "GetArchiveCommand: failed to retrieve archive_command")
	}
	return archiveCommand, nil
}

// IsStandby checks if the PostgreSQL server is in recovery mode (standby).
func (queryRunner *PgQueryRunner) IsStandby(ctx context.Context) (bool, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	var standby bool
	err := queryRunner.Connection.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&standby)
	if err != nil {
		return false, errors.Wrap(err, "IsStandby: failed to determine recovery mode")
	}
	return standby, nil
}
