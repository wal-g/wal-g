package postgres

import (
	"os"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal"
)

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	BackupStartLSN    *uint64 `json:"LSN"`
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	Files       internal.BackupFileList `json:"Files"`
	TarFileSets TarFileSets             `json:"TarFileSets"`

	PgVersion        int     `json:"PgVersion"`
	BackupFinishLSN  *uint64 `json:"FinishLSN"`
	SystemIdentifier *uint64 `json:"SystemIdentifier,omitempty"`

	UncompressedSize int64           `json:"UncompressedSize"`
	CompressedSize   int64           `json:"CompressedSize"`
	TablespaceSpec   *TablespaceSpec `json:"Spec"`

	UserData interface{} `json:"UserData,omitempty"`
}

func NewBackupSentinelDto(bh *BackupHandler, tbsSpec *TablespaceSpec, tarFileSets TarFileSets) BackupSentinelDto {
	sentinel := BackupSentinelDto{
		BackupStartLSN:   &bh.curBackupInfo.startLSN,
		IncrementFromLSN: bh.prevBackupInfo.sentinelDto.BackupStartLSN,
		PgVersion:        bh.pgInfo.pgVersion,
		TablespaceSpec:   tbsSpec,
	}
	if bh.prevBackupInfo.sentinelDto.BackupStartLSN != nil {
		sentinel.IncrementFrom = &bh.prevBackupInfo.name
		if bh.prevBackupInfo.sentinelDto.IsIncremental() {
			sentinel.IncrementFullName = bh.prevBackupInfo.sentinelDto.IncrementFullName
		} else {
			sentinel.IncrementFullName = &bh.prevBackupInfo.name
		}
		sentinel.IncrementCount = &bh.curBackupInfo.incrementCount
	}

	sentinel.BackupFinishLSN = &bh.curBackupInfo.endLSN
	sentinel.UserData = internal.UnmarshalSentinelUserData(bh.arguments.userData)
	sentinel.SystemIdentifier = bh.pgInfo.systemIdentifier
	sentinel.UncompressedSize = bh.curBackupInfo.uncompressedSize
	sentinel.CompressedSize = bh.curBackupInfo.compressedSize
	sentinel.TarFileSets = tarFileSets
	return sentinel
}

// Extended metadata should describe backup in more details, but be small enough to be downloaded often
type ExtendedMetadataDto struct {
	StartTime        time.Time `json:"start_time"`
	FinishTime       time.Time `json:"finish_time"`
	DatetimeFormat   string    `json:"date_fmt"`
	Hostname         string    `json:"hostname"`
	DataDir          string    `json:"data_dir"`
	PgVersion        int       `json:"pg_version"`
	StartLsn         uint64    `json:"start_lsn"`
	FinishLsn        uint64    `json:"finish_lsn"`
	IsPermanent      bool      `json:"is_permanent"`
	SystemIdentifier *uint64   `json:"system_identifier"`

	UncompressedSize int64 `json:"uncompressed_size"`
	CompressedSize   int64 `json:"compressed_size"`

	UserData interface{} `json:"user_data,omitempty"`
}

func NewExtendedMetadataDto(isPermanent bool, dataDir string, startTime time.Time,
	sentinelDto BackupSentinelDto) (meta ExtendedMetadataDto) {
	hostname, err := os.Hostname()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to fetch the hostname for metadata, leaving empty: %v", err)
	}
	meta.DatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"
	meta.StartTime = startTime
	meta.FinishTime = utility.TimeNowCrossPlatformUTC()
	meta.Hostname = hostname
	meta.IsPermanent = isPermanent
	meta.DataDir = dataDir

	// set the matching fields from sentinel
	meta.StartLsn = *sentinelDto.BackupStartLSN
	meta.FinishLsn = *sentinelDto.BackupFinishLSN
	meta.PgVersion = sentinelDto.PgVersion
	meta.SystemIdentifier = sentinelDto.SystemIdentifier
	meta.UserData = sentinelDto.UserData
	meta.UncompressedSize = sentinelDto.UncompressedSize
	meta.CompressedSize = sentinelDto.CompressedSize
	return meta
}

func (dto *BackupSentinelDto) setFiles(p *sync.Map) {
	dto.Files = make(internal.BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(internal.BackupFileDescription)
		dto.Files[key] = description
		return true
	})
}

// TODO : unit tests
// TODO : get rid of panic here
// IsIncremental checks that sentinel represents delta backup
func (dto *BackupSentinelDto) IsIncremental() (isIncremental bool) {
	// If we have increment base, we must have all the rest properties.
	if dto.IncrementFrom != nil {
		if dto.IncrementFromLSN == nil || dto.IncrementFullName == nil || dto.IncrementCount == nil {
			panic("Inconsistent BackupSentinelDto")
		}
	}
	return dto.IncrementFrom != nil
}
