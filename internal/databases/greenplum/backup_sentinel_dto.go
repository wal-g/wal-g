package greenplum

import (
	"encoding/json"

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
}

func (s *BackupSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

// NewBackupSentinelDto returns new BackupSentinelDto instance
func NewBackupSentinelDto(curBackupInfo CurBackupInfo, restoreLSNs map[int]string) BackupSentinelDto {
	sentinel := BackupSentinelDto{
		RestorePoint: &curBackupInfo.backupName,
		Segments:     make([]SegmentMetadata, 0, len(curBackupInfo.segmentBackups)),
	}

	for backupID, cfg := range curBackupInfo.segmentBackups {
		restoreLSN := restoreLSNs[cfg.ContentID]
		sentinel.Segments = append(sentinel.Segments, NewSegmentMetadata(backupID, *cfg, restoreLSN))
	}
	return sentinel
}
