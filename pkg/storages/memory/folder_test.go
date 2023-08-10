package memory

import (
	"testing"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestMemoryFolder(t *testing.T) {
	storage.RunFolderTest(NewFolder("in_memory/", NewStorage()), t)
}
