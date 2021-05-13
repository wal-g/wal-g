package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/memory"
	"github.com/wal-g/storages/storage"
	"testing"
	"time"

	"github.com/wal-g/wal-g/internal"
)

func TestGetGarbageFromPrefix(t *testing.T) {
	backupNames := []string{"backup", "garbage", "garbage_0"}
	folders := make([]storage.Folder, 0)
	nonGarbage := []internal.BackupTime{{"backup", time.Now(), "some_postfix.json"}}

	for _, prefix := range backupNames {
		folders = append(folders, memory.NewFolder(prefix, memory.NewStorage()))
	}

	garbage := internal.GetGarbageFromPrefix(folders, nonGarbage)
	assert.Equal(t, garbage, []string{"garbage", "garbage_0"})
}