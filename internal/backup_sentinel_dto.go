package internal

import (
	"sync"
	"time"
)

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	BackupStartLSN    *uint64 `json:"LSN"`
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	Files       BackupFileList      `json:"Files"`
	TarFileSets map[string][]string `json:"TarFileSets"`

	PgVersion       int     `json:"PgVersion"`
	BackupFinishLSN *uint64 `json:"FinishLSN"`

	UserData interface{} `json:"UserData,omitempty"`
}

// Extended metadata should describe backup in more details, but be small enough to be downloaded often
type ExtendedMetadataDto struct {
	StartTime      				time.Time 	`json:"start_time"`
	FinishTime     				time.Time 	`json:"finish_time"`
	DatetimeFormat 				string    	`json:"date_fmt"`
	Hostname       				string    	`json:"hostname"`
	DataDir        				string    	`json:"data_dir"`
	PgVersion      				int       	`json:"pg_version"`
	StartLsn       				uint64    	`json:"start_lsn"`
	FinishLsn      				uint64    	`json:"finish_lsn"`
	IsPermanent    				bool      	`json:"is_permanent"`
	HasPermanentInFuture		bool		`json:"has_permanent_in_future"`
}

func (dto *BackupSentinelDto) setFiles(p *sync.Map) {
	dto.Files = make(BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(BackupFileDescription)
		dto.Files[key] = description
		return true
	})
}

// TODO : unit tests
// TODO : get rid of panic here
// IsIncremental checks that sentinel represents delta backup
func (dto *BackupSentinelDto) IsIncremental() bool {
	// If we have increment base, we must have all the rest properties.
	if dto.IncrementFrom != nil {
		if dto.IncrementFromLSN == nil || dto.IncrementFullName == nil || dto.IncrementCount == nil {
			panic("Inconsistent BackupSentinelDto")
		}
	}
	return dto.IncrementFrom != nil
}
