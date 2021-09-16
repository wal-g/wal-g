package greenplum

import (
	"encoding/json"
	"os"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type SegmentRole string

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

func NewSegmentMetadata(backupID string, segCfg cluster.SegConfig, restoreLSN string) SegmentMetadata {
	return SegmentMetadata{
		DatabaseID:      segCfg.DbID,
		ContentID:       segCfg.ContentID,
		Role:            SegmentRole(segCfg.Role),
		Port:            segCfg.Port,
		Hostname:        segCfg.Hostname,
		DataDir:         segCfg.DataDir,
		BackupID:        backupID,
		RestorePointLSN: restoreLSN,
	}
}

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	RestorePoint *string           `json:"restore_point,omitempty"`
	Segments     []SegmentMetadata `json:"segments,omitempty"`
	UserData     interface{}       `json:"user_data,omitempty"`

	StartTime        time.Time `json:"start_time"`
	FinishTime       time.Time `json:"finish_time"`
	Hostname         string    `json:"hostname"`
	GpVersion        string    `json:"gp_version"`
	IsPermanent      bool      `json:"is_permanent"`
	SystemIdentifier *uint64   `json:"system_identifier"`

	UncompressedSize int64 `json:"uncompressed_size"`
	CompressedSize   int64 `json:"compressed_size"`
}

func (s *BackupSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

// NewBackupSentinelDto returns new BackupSentinelDto instance
func NewBackupSentinelDto(currBackupInfo CurrBackupInfo, restoreLSNs map[int]string, userData interface{},
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
		Hostname:         hostname,
		GpVersion:        currBackupInfo.gpVersion.String(),
		IsPermanent:      isPermanent,
		SystemIdentifier: currBackupInfo.systemIdentifier,
	}

	for idx := range currBackupInfo.segmentsMetadata {
		sentinel.CompressedSize += currBackupInfo.segmentsMetadata[idx].CompressedSize
		sentinel.UncompressedSize += currBackupInfo.segmentsMetadata[idx].UncompressedSize
	}

	for backupID, cfg := range currBackupInfo.segmentBackups {
		restoreLSN := restoreLSNs[cfg.ContentID]
		sentinel.Segments = append(sentinel.Segments, NewSegmentMetadata(backupID, *cfg, restoreLSN))
	}
	return sentinel
}
