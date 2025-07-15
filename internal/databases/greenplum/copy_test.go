package greenplum_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
)

func TestGetCopyingInfos_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	infos, err := greenplum.GetCopyingInfos("backup_20241212T061346Z", from, to)
	assert.Error(t, err)
	assert.Empty(t, infos)
}

func TestGetHistoryCopyingInfo_WhenFolderIsNotEmpty(t *testing.T) {
	postgres.SetWalSize(64)
	var from = testtools.CreateMockStorageFolderWithPermanentGPBackups(t)

	var to = testtools.MakeDefaultInMemoryStorageFolder()
	infos, err := greenplum.GetCopyingInfos("backup_20241212T061346Z", from, to)
	assert.NoError(t, err)
	for _, foo := range infos {
		tracelog.InfoLogger.Printf("%s", foo.SrcObj.GetName())
	}
	assert.Equal(t, 26, len(infos))
}
