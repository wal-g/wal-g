package greenplum

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/wal-g/wal-g/internal/walparser"

	"github.com/pkg/errors"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// GpQueryRunner is implementation for controlling Greenplum
type GpQueryRunner struct {
	*postgres.PgQueryRunner
}

type aoRelPgClassInfo struct {
	oid           uint32
	relNameMd5    string
	relFileNodeID uint32
	relNAtts      int16
	spcNode       uint32
	storage       RelStorageType
}

// NewGpQueryRunner builds QueryRunner from available connection
func NewGpQueryRunner(conn *pgx.Conn) (*GpQueryRunner, error) {
	pgQueryRunner, err := postgres.NewPgQueryRunner(conn)
	if err != nil {
		return nil, err
	}
	r := &GpQueryRunner{PgQueryRunner: pgQueryRunner}
	return r, nil
}

func ToGpQueryRunner(queryRunner *postgres.PgQueryRunner) *GpQueryRunner {
	return &GpQueryRunner{PgQueryRunner: queryRunner}
}

// BuildCreateGreenplumRestorePoint formats a query to create a restore point
func (queryRunner *GpQueryRunner) buildCreateGreenplumRestorePoint(restorePointName string) string {
	return fmt.Sprintf("SELECT (gp_create_restore_point('%s'))::text", restorePointName)
}

// CreateGreenplumRestorePoint creates a restore point
func (queryRunner *GpQueryRunner) CreateGreenplumRestorePoint(restorePointName string) (restoreLSNs map[int]string, err error) {
	conn := queryRunner.Connection
	rows, err := conn.Query(queryRunner.buildCreateGreenplumRestorePoint(restorePointName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	restoreLSNs = make(map[int]string)
	restorePointPattern := regexp.MustCompile(`(-?\d+),([0-9A-F]+/[0-9A-F]+)`)
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			tracelog.WarningLogger.Printf("CreateGreenplumRestorePoint:  %v\n", err.Error())
		}
		match := restorePointPattern.FindStringSubmatch(row)
		if match == nil {
			return nil, fmt.Errorf("failed to parse CreateGreenplumRestorePoint output row: %s", row)
		}
		contentID, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse the contentID value (%s)", match[1])
		}
		restoreLSNs[contentID] = match[2]
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return restoreLSNs, nil
}

// BuildGetGreenplumSegmentsInfo formats a query to retrieve information about segments
func (queryRunner *GpQueryRunner) buildGetGreenplumSegmentsInfo(semVer semver.Version) string {
	validRange := dbconn.StringToSemVerRange("<6")
	if validRange(semVer) {
		return `
SELECT
	s.dbid,
	s.content,
	s.role::text,
	s.port,
	s.hostname,
	e.fselocation
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content, s.role DESC;`
	}
	return `
SELECT
	dbid,
	content,
	role::text,
	port,
	hostname,
	datadir
FROM gp_segment_configuration
WHERE role = 'p'
ORDER BY content, role DESC;`
}

// GetGreenplumSegmentsInfo returns the information about segments
func (queryRunner *GpQueryRunner) GetGreenplumSegmentsInfo(semVer semver.Version) (segments []cluster.SegConfig, err error) {
	conn := queryRunner.Connection
	rows, err := conn.Query(queryRunner.buildGetGreenplumSegmentsInfo(semVer))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	segments = make([]cluster.SegConfig, 0)
	for rows.Next() {
		var dbID int
		var contentID int
		var role string
		var port int
		var hostname string
		var dataDir string
		if err := rows.Scan(&dbID, &contentID, &role, &port, &hostname, &dataDir); err != nil {
			tracelog.WarningLogger.Printf("GetGreenplumSegmentsInfo:  %v\n", err.Error())
		}
		segment := cluster.SegConfig{
			DbID:      dbID,
			ContentID: contentID,
			Role:      role,
			Port:      port,
			Hostname:  hostname,
			DataDir:   dataDir,
		}
		segments = append(segments, segment)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return segments, nil
}

// GetGreenplumVersion returns version
func (queryRunner *GpQueryRunner) GetGreenplumVersion() (version string, err error) {
	conn := queryRunner.Connection
	err = conn.QueryRow("SELECT pg_catalog.version()").Scan(&version)
	if err != nil {
		return "", err
	}
	return version, nil
}

// BuildIsInBackup formats a query to retrieve information about running backups
func (queryRunner *GpQueryRunner) buildIsInBackup() string {
	return `
SELECT pg_is_in_backup(), gp_segment_id FROM gp_dist_random('gp_id')
UNION ALL
SELECT pg_is_in_backup(), -1;
`
}

// buildAbortBackupSegments aborts the running backup on the segments
func (queryRunner *GpQueryRunner) buildAbortBackupSegments() string {
	return `SELECT pg_stop_backup(), gp_segment_id FROM gp_dist_random('gp_id');`
}

// buildAbortBackupSegments aborts the running backup on the master instance
func (queryRunner *GpQueryRunner) buildAbortBackupMaster() string {
	return `SELECT pg_stop_backup();`
}

// IsInBackup check if there is backup running
func (queryRunner *GpQueryRunner) IsInBackup() (isInBackupByContentID map[int]bool, err error) {
	conn := queryRunner.Connection

	rows, err := conn.Query(queryRunner.buildIsInBackup())
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner IsInBackup: query failed")
	}

	defer rows.Close()
	results := make(map[int]bool)
	for rows.Next() {
		var contentID int
		var isInBackup bool
		if err := rows.Scan(&isInBackup, &contentID); err != nil {
			tracelog.WarningLogger.Printf("QueryRunner IsInBackup:  %v\n", err.Error())
		}
		results[contentID] = isInBackup
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return results, nil
}

// AbortBackup stops the backup process on all segments
func (queryRunner *GpQueryRunner) AbortBackup() (err error) {
	tracelog.InfoLogger.Println("Calling pg_stop_backup() on all segments...")
	conn := queryRunner.Connection

	errs := make([]error, 0)
	_, err = conn.Exec("SET statement_timeout=0;")
	if err != nil {
		errs = append(errs, errors.Wrap(err, "QueryRunner AbortBackup: failed setting statement timeout in transaction"))
	}

	_, err = conn.Exec(queryRunner.buildAbortBackupSegments())
	if err != nil {
		errs = append(errs, errors.Wrap(err, "QueryRunner IsInBackup: segment backups stop error"))
	}

	_, err = conn.Exec(queryRunner.buildAbortBackupMaster())
	if err != nil {
		errs = append(errs, errors.Wrap(err, "QueryRunner IsInBackup: master backup stop error"))
	}

	var finalErr error
	for i := range errs {
		finalErr = errors.Wrap(finalErr, errs[i].Error())
	}

	return finalErr
}

// FetchAOStorageMetadata queries the storage metadata for AO & AOCS tables (GreenplumDB)
func (queryRunner *GpQueryRunner) FetchAOStorageMetadata(dbInfo postgres.PgDatabaseInfo) (AoRelFileStorageMap, error) {
	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()

	tracelog.InfoLogger.Printf("Querying pg_class for %s", dbInfo.Name)
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
				row.storage, row.relFileNodeID, dbInfo.Name)
			continue
		}

		err = loadStorageMetadata(relStorageMap, dbInfo, queryFunc, aoSegTableFqn, relPgClassInfo)
		if err != nil {
			tracelog.WarningLogger.Printf("failed to fetch the AOCS storage metadata: %v\n", err)
		}
	}

	return relStorageMap, nil
}

func loadStorageMetadata(relStorageMap AoRelFileStorageMap, dbInfo postgres.PgDatabaseInfo,
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
		relFileLoc := walparser.NewBlockLocation(walparser.Oid(cInfo.spcNode), dbInfo.Oid, walparser.Oid(cInfo.relFileNodeID), uint32(segNo))
		// if tablespace id is zero, use the default database tablespace id
		if relFileLoc.RelationFileNode.SpcNode == walparser.Oid(0) {
			relFileLoc.RelationFileNode.SpcNode = dbInfo.TblSpcOid
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

func (queryRunner *GpQueryRunner) buildAORelPgClassQuery() (string, error) {
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
		return "", postgres.NewNoPostgresVersionError()
	default:
		return "", postgres.NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

func (queryRunner *GpQueryRunner) buildAOCSMetadataQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return `SELECT aocs.physical_segno as segno, aocs.modcount, aocs.eof " +
			"FROM gp_toolkit.__gp_aocsseg($1::oid) aocs;`, nil
	case queryRunner.Version == 0:
		return "", postgres.NewNoPostgresVersionError()
	default:
		return "", postgres.NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

func (queryRunner *GpQueryRunner) buildAOMetadataQuery(aoSegTableFqn string) (string, error) {
	switch {
	case queryRunner.Version >= 90000:
		return fmt.Sprintf(`SELECT segno, modcount, eof FROM %s;`, aoSegTableFqn), nil
	case queryRunner.Version == 0:
		return "", postgres.NewNoPostgresVersionError()
	default:
		return "", postgres.NewUnsupportedPostgresVersionError(queryRunner.Version)
	}
}
