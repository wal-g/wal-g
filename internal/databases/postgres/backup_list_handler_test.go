package postgres_test

import (
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
)

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	postgres.HandleBackupListWithFlags(folder, true, false, false)
}
