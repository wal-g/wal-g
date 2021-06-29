package greenplum

import (
	"fmt"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// GpQueryRunner is implementation for controlling Greenplum
type GpQueryRunner struct {
	pgQueryRunner *postgres.PgQueryRunner
}

// NewPgQueryRunner builds QueryRunner from available connection
func NewPgQueryRunner(conn *pgx.Conn) (*GpQueryRunner, error) {
	pgQueryRunner, err := postgres.NewPgQueryRunner(conn)
	if err != nil {
		return nil, err
	}
	r := &GpQueryRunner{pgQueryRunner: pgQueryRunner}
	return r, nil
}

// BuildCreateGreenplumRestorePoint formats a query to create a restore point
func (queryRunner *GpQueryRunner) buildCreateGreenplumRestorePoint(restorePointName string) string {
	return fmt.Sprintf("SELECT (gp_create_restore_point('%s'))::text", restorePointName)
}

// CreateGreenplumRestorePoint creates a restore point
func (queryRunner *GpQueryRunner) CreateGreenplumRestorePoint(restorePointName string) (lsnStrings []string, err error) {
	conn := queryRunner.pgQueryRunner.Connection
	rows, err := conn.Query(queryRunner.buildCreateGreenplumRestorePoint(restorePointName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	lsnStrings = make([]string, 0)
	for rows.Next() {
		var lsn string

		if err := rows.Scan(&lsn); err != nil {
			tracelog.WarningLogger.Printf("CreateGreenplumRestorePoint:  %v\n", err.Error())
		}
		lsnStrings = append(lsnStrings, lsn)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return lsnStrings, nil
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
	conn := queryRunner.pgQueryRunner.Connection
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
	conn := queryRunner.pgQueryRunner.Connection
	err = conn.QueryRow("SELECT pg_catalog.version()").Scan(&version)
	if err != nil {
		return "", err
	}
	return version, nil
}
