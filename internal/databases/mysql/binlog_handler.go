package mysql

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleBinlogForgetGtids(uploader internal.UploaderProvider) {
	rootFolder := uploader.Folder()
	uploader.ChangeDirectory(BinlogPath)

	// Remove GTIDs from remote sentinel
	var binlogSentinelDto BinlogSentinelDto
	err := FetchBinlogSentinel(rootFolder, &binlogSentinelDto)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogSentinelDto.GTIDArchived = ""

	err = UploadBinlogSentinel(rootFolder, &binlogSentinelDto)
	tracelog.ErrorLogger.FatalOnError(err)

	// clean local cache:
	cache := getCache()
	cache.GTIDArchived = ""
	putCache(cache)
	tracelog.InfoLogger.Printf("Binlog sentinel: %s, cache: %+v", binlogSentinelDto.String(), cache)
}
