package postgres

import (
	"os"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal"
)

const MetadataDatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	BackupStartLSN    *LSN    `json:"LSN"`
	IncrementFromLSN  *LSN    `json:"DeltaLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	PgVersion        int     `json:"PgVersion"`
	BackupFinishLSN  *LSN    `json:"FinishLSN"`
	SystemIdentifier *uint64 `json:"SystemIdentifier,omitempty"`

	UncompressedSize int64           `json:"UncompressedSize"`
	CompressedSize   int64           `json:"CompressedSize"`
	DataCatalogSize  int64           `json:"DataCatalogSize,omitempty"`
	TablespaceSpec   *TablespaceSpec `json:"Spec"`

	UserData interface{} `json:"UserData,omitempty"`

	FilesMetadataDisabled bool `json:"FilesMetadataDisabled,omitempty"`
}

func NewBackupSentinelDto(bh *BackupHandler, tbsSpec *TablespaceSpec) BackupSentinelDto {
	sentinel := BackupSentinelDto{
		BackupStartLSN:   &bh.CurBackupInfo.startLSN,
		IncrementFromLSN: bh.prevBackupInfo.sentinelDto.BackupStartLSN,
		PgVersion:        bh.PgInfo.pgVersion,
		TablespaceSpec:   tbsSpec,
	}
	if bh.prevBackupInfo.sentinelDto.BackupStartLSN != nil {
		sentinel.IncrementFrom = &bh.prevBackupInfo.name
		if bh.prevBackupInfo.sentinelDto.IsIncremental() {
			sentinel.IncrementFullName = bh.prevBackupInfo.sentinelDto.IncrementFullName
		} else {
			sentinel.IncrementFullName = &bh.prevBackupInfo.name
		}
		sentinel.IncrementCount = &bh.CurBackupInfo.incrementCount
	}

	sentinel.BackupFinishLSN = &bh.CurBackupInfo.endLSN
	sentinel.UserData = bh.Arguments.userData
	sentinel.SystemIdentifier = bh.PgInfo.systemIdentifier
	sentinel.UncompressedSize = bh.CurBackupInfo.uncompressedSize
	sentinel.CompressedSize = bh.CurBackupInfo.compressedSize
	sentinel.DataCatalogSize = bh.CurBackupInfo.dataCatalogSize
	sentinel.FilesMetadataDisabled = bh.Arguments.withoutFilesMetadata
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
	StartLsn         LSN       `json:"start_lsn"`
	FinishLsn        LSN       `json:"finish_lsn"`
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
	meta.DatetimeFormat = MetadataDatetimeFormat
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

// FilesMetadataDto contains the information about the backup files.
// It can be pretty large on some databases, sometimes more than 1GB
type FilesMetadataDto struct {
	Files            internal.BackupFileList `json:"Files,omitempty"`
	TarFileSets      map[string][]string     `json:"TarFileSets,omitempty"`
	DatabasesByNames DatabasesByNames        `json:"DatabasesByNames,omitempty"`
}

func NewFilesMetadataDto(files internal.BackupFileList, tarFileSets internal.TarFileSets) FilesMetadataDto {
	return FilesMetadataDto{TarFileSets: tarFileSets.Get(), Files: files}
}

func (dto *FilesMetadataDto) setFiles(p *sync.Map) {
	dto.Files = make(internal.BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(internal.BackupFileDescription)
		dto.Files[key] = description
		return true
	})
}

// BackupSentinelDtoV2 is the future version of the backup sentinel.
// Basically, it is a union of BackupSentinelDto and ExtendedMetadataDto.
// Currently, WAL-G only uploads it, but use as the regular BackupSentinelDto.
// WAL-G will switch to the BackupSentinelDtoV2 in the next major release.
type BackupSentinelDtoV2 struct {
	BackupSentinelDto
	Version        int       `json:"Version"`
	StartTime      time.Time `json:"StartTime"`
	FinishTime     time.Time `json:"FinishTime"`
	DatetimeFormat string    `json:"DateFmt"`
	Hostname       string    `json:"Hostname"`
	DataDir        string    `json:"DataDir"`
	IsPermanent    bool      `json:"IsPermanent"`
}

func NewBackupSentinelDtoV2(sentinel BackupSentinelDto, meta ExtendedMetadataDto) BackupSentinelDtoV2 {
	return BackupSentinelDtoV2{
		BackupSentinelDto: sentinel,
		Version:           2,
		StartTime:         meta.StartTime,
		FinishTime:        meta.FinishTime,
		DatetimeFormat:    meta.DatetimeFormat,
		Hostname:          meta.Hostname,
		DataDir:           meta.DataDir,
		IsPermanent:       meta.IsPermanent,
	}
}

type DeprecatedSentinelFields struct {
	FilesMetadataDto
	DeltaFromLSN *LSN `json:"DeltaFromLSN,omitempty"`
}
