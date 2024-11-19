package xbstream

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"gopkg.in/ini.v1"
)

// data format for IBD file:
//
//	page_size = 16384
//	zip_size = 0
//	space_id = 0
//	space_flags = 18432
//
// data format for undo log file:
//
//	page_size = 16384
//	zip_size = 0
//	space_id = 4294967279  // 0xffffffef, however it is not constant
//	space_flags = 0
type diffMetadata struct {
	PageSize   uint32         `ini:"page_size"`
	ZipSize    uint64         `ini:"zip_size"`
	SpaceID    innodb.SpaceID `ini:"space_id"`
	SpaceFlags uint32         `ini:"space_flags"`
}

func parseDiffMetadata(rows []byte) (diffMetadata, error) {
	result := diffMetadata{}

	cfg, err := ini.Load(rows)
	if err != nil {
		return diffMetadata{}, err
	}
	err = cfg.MapTo(&result)
	if err != nil {
		return diffMetadata{}, err
	}

	if result.PageSize > 64*1024 {
		tracelog.ErrorLogger.Fatalf("page_size in diff is greater than supported. page_size = %v", result.PageSize)
	}
	return result, nil
}
