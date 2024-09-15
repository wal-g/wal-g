package xbstream

import (
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"strconv"
	"strings"
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
	pageSize   uint16
	zipSize    uint64
	spaceId    innodb.SpaceID
	spaceFlags uint32
}

func parseDiffMetadata(rows string) (diffMetadata, error) {
	result := diffMetadata{}
	for _, row := range strings.Split(rows, "\n") {
		if row == "" {
			continue
		}
		pair := strings.SplitN(row, "=", 2)
		if len(pair) != 2 {
			return diffMetadata{}, fmt.Errorf("Invalid metadata format: cannot parse row '%v'", row)
		}
		key := strings.TrimSpace(pair[0])
		value := strings.TrimSpace(pair[1])

		switch key {
		case "page_size":
			size, err := strconv.ParseUint(value, 10, 16)
			if err != nil {
				return diffMetadata{}, err
			}
			if size > 64*1024 {
				tracelog.ErrorLogger.Fatalf("page_size in diff is greater than supported. page_size = %v", size)
			}
			result.pageSize = uint16(size)
		case "zip_size":
			size, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return diffMetadata{}, err
			}
			result.zipSize = size
		case "space_id":
			spaceId, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return diffMetadata{}, err
			}
			result.spaceId = innodb.SpaceID(spaceId)
		case "space_flags":
			spaceFlags, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return diffMetadata{}, err
			}
			result.spaceFlags = uint32(spaceFlags)
		default:
			tracelog.WarningLogger.Printf("Unknown metadata key observerd: %v = %v", key, value)
		}

	}

	return result, nil
}
