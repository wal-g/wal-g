package clickhouse

import (
	"encoding/json"
	"time"
)

type BackupSentinelDto struct {
	StartLocalTime time.Time `json:"StartLocalTime,omitempty"`
	StopLocalTime  time.Time `json:"StopLocalTime,omitempty"`

	UncompressedSize int64  `json:"UncompressedSize,omitempty"`
	CompressedSize   int64  `json:"CompressedSize,omitempty"`
	Hostname         string `json:"Hostname,omitempty"`

	IsPermanent bool `json:"IsPermanent,omitempty"`
}

func (s *BackupSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}
