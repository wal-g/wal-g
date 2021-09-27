package greenplum

import (
	"fmt"
	"regexp"
	"strconv"

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
	pgQueryRunner *postgres.PgQueryRunner
}

// NewGpQueryRunner builds QueryRunner from available connection
func NewGpQueryRunner(conn *pgx.Conn) (*GpQueryRunner, error) {
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
func (queryRunner *GpQueryRunner) CreateGreenplumRestorePoint(restorePointName string) (restoreLSNs map[int]string, err error) {
	conn := queryRunner.pgQueryRunner.Connection
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

// BuildIsInBackup formats a query to retrieve information about running backups
func (queryRunner *GpQueryRunner) buildIsInBackup() string {
	return `
SELECT pg_is_in_backup(), gp_segment_id FROM gp_dist_random('gp_id')
UNION ALL
SELECT pg_is_in_backup(), -1;
`
}

// IsInBackup check if there is backup running
func (queryRunner *GpQueryRunner) IsInBackup() (isInBackupByContentID map[int]bool, err error) {
	conn := queryRunner.pgQueryRunner.Connection

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
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return results, nil
}
