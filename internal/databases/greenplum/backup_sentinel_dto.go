package greenplum

import (
	"encoding/json"
	"os"
	"time"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type SegmentRole string

const MetadataDatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

const (
	Primary SegmentRole = "p"
	Mirror  SegmentRole = "m"
)

type SegmentMetadata struct {
	DatabaseID int         `json:"db_id"`
	ContentID  int         `json:"content_id"`
	Role       SegmentRole `json:"role"`
	Port       int         `json:"port"`
	Hostname   string      `json:"hostname"`
	DataDir    string      `json:"data_dir"`

	BackupID        string `json:"backup_id"`
	BackupName      string `json:"backup_name"`
	RestorePointLSN string `json:"restore_point_lsn"`
}

func (c SegmentMetadata) ToSegConfig() cluster.SegConfig {
	return cluster.SegConfig{
		DbID:      c.DatabaseID,
		ContentID: c.ContentID,
		Role:      string(c.Role),
		Port:      c.Port,
		Hostname:  c.Hostname,
		DataDir:   c.DataDir,
	}
}

func NewSegmentMetadata(backupID string, segCfg cluster.SegConfig, restoreLSN, backupName string) SegmentMetadata {
	return SegmentMetadata{
		DatabaseID:      segCfg.DbID,
		ContentID:       segCfg.ContentID,
		Role:            SegmentRole(segCfg.Role),
		Port:            segCfg.Port,
		Hostname:        segCfg.Hostname,
		DataDir:         segCfg.DataDir,
		BackupID:        backupID,
		RestorePointLSN: restoreLSN,
		BackupName:      backupName,
	}
}

// PgSegmentSentinelDto is used during the initial fetching of the segment backup metadata
type PgSegmentSentinelDto struct {
	postgres.BackupSentinelDto
	BackupName string
}

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	RestorePoint *string           `json:"restore_point,omitempty"`
	Segments     []SegmentMetadata `json:"segments,omitempty"`
	UserData     interface{}       `json:"user_data,omitempty"`

	StartTime        time.Time `json:"start_time"`
	FinishTime       time.Time `json:"finish_time"`
	DatetimeFormat   string    `json:"date_fmt,omitempty"`
	Hostname         string    `json:"hostname"`
	GpVersion        string    `json:"gp_version"`
	IsPermanent      bool      `json:"is_permanent"`
	SystemIdentifier *uint64   `json:"system_identifier"`

	UncompressedSize int64 `json:"uncompressed_size"`
	CompressedSize   int64 `json:"compressed_size"`
	DataCatalogSize  int64 `json:"data_catalog_size"`

	IncrementFrom     *string `json:"increment_from,omitempty"`
	IncrementFullName *string `json:"increment_full_name,omitempty"`
	IncrementCount    *int    `json:"increment_count,omitempty"`
}

func (s *BackupSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

// NewBackupSentinelDto returns new BackupSentinelDto instance
func NewBackupSentinelDto(currBackupInfo CurrBackupInfo, prevBackupInfo *PrevBackupInfo, restoreLSNs map[int]string, userData interface{},
	isPermanent bool) BackupSentinelDto {
	hostname, err := os.Hostname()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to fetch the hostname for metadata, leaving empty: %v", err)
	}

	sentinel := BackupSentinelDto{
		RestorePoint:     &currBackupInfo.backupName,
		Segments:         make([]SegmentMetadata, 0, len(currBackupInfo.segmentBackups)),
		UserData:         userData,
		StartTime:        currBackupInfo.startTime,
		FinishTime:       utility.TimeNowCrossPlatformUTC(),
		DatetimeFormat:   MetadataDatetimeFormat,
		Hostname:         hostname,
		GpVersion:        currBackupInfo.gpVersion.String(),
		IsPermanent:      isPermanent,
		SystemIdentifier: currBackupInfo.systemIdentifier,
	}

	if prevBackupInfo.name != "" {
		sentinel.IncrementCount = &currBackupInfo.incrementCount
		sentinel.IncrementFrom = &prevBackupInfo.name
		if prevBackupInfo.sentinelDto.IsIncremental() {
			sentinel.IncrementFullName = prevBackupInfo.sentinelDto.IncrementFullName
		} else {
			sentinel.IncrementFullName = &prevBackupInfo.name
		}
	}

	for backupID := range currBackupInfo.segmentsMetadata {
		sentinel.CompressedSize += currBackupInfo.segmentsMetadata[backupID].CompressedSize
		sentinel.UncompressedSize += currBackupInfo.segmentsMetadata[backupID].UncompressedSize
		sentinel.DataCatalogSize += currBackupInfo.segmentsMetadata[backupID].DataCatalogSize
	}

	for backupID, cfg := range currBackupInfo.segmentBackups {
		restoreLSN := restoreLSNs[cfg.ContentID]
		backupName := currBackupInfo.segmentsMetadata[backupID].BackupName
		sentinel.Segments = append(sentinel.Segments, NewSegmentMetadata(backupID, *cfg, restoreLSN, backupName))
	}
	return sentinel
}

func (s *BackupSentinelDto) IsIncremental() (isIncremental bool) {
	// If we have increment base, we must have all the rest properties.
	if s.IncrementFrom != nil {
		if s.IncrementFullName == nil || s.IncrementCount == nil {
			panic("Inconsistent BackupSentinelDto")
		}
	}
	return s.IncrementFrom != nil
}
