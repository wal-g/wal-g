package internal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

func generateFolders() (from storage.Object, to storage.Object) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to   = testtools.MakeDefaultInMemoryStorageFolder()
	return
}


//
func TestGetBackupObjects(t *testing.T) {

}

// просто зафигать каких-нибудь history
// и проверить, что они правильно скопированы
func TestGetHistoryObjects(t *testing.T) {

}

/*	1. from, to пустые
 *  2. в from непусто
 *  3. в to непусто
 *  4. from и to непустые
 */
func TestGetAllObjects(t *testing.T) {

}

/*
 * 1. пустые
 * 2. вложенность 1
 * 3. вложенность 2
 * 4. condition false
 * 5. condition true
 * 6. condition сложный
 */
func TestBuildCopyingInfos(t *testing.T) {
}

func TestBuildCopyingInfos_EmptyFolders_NoObjects(t *testing.T) {
	var from, to = generateFolders()
	var infos = internal.BuildCopyingInfos(from, to, [], func(object storage.Object) bool { return true })
	assert.Equal(t, 0, len(infos))

}
