package postgres_test

import (
	"github.com/wal-g/wal-g/utility"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
)

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	postgres.HandleBackupListWithFlags(folder.GetSubFolder(utility.BaseBackupPath), true, false, false)
}
