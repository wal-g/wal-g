package walg

import "sync"

// S3TarBallSentinelDto describes file structure of json sentinel
type S3TarBallSentinelDto struct {
	LSN               *uint64
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	Files BackupFileList

	PgVersion int
	FinishLSN *uint64

	UserData interface{} `json:"UserData,omitempty"`
}

func (dto *S3TarBallSentinelDto) SetFiles(p *sync.Map) {
	dto.Files = make(BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(BackupFileDescription)
		dto.Files[key] = description
		return true
	})
}

// IsIncremental checks that sentinel represents delta backup
func (dto *S3TarBallSentinelDto) IsIncremental() bool {
	// If we have increment base, we must have all the rest properties.
	// If we do not have base - anything else is a mistake
	if dto.IncrementFrom != nil {
		if dto.IncrementFromLSN == nil || dto.IncrementFullName == nil || dto.IncrementCount == nil {
			panic("Inconsistent S3TarBallSentinelDto")
		}
	} else if dto.IncrementFromLSN != nil && dto.IncrementFullName != nil && dto.IncrementCount != nil {
		panic("Inconsistent S3TarBallSentinelDto")
	}
	return dto.IncrementFrom != nil
}