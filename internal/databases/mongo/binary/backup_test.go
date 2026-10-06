package binary

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
)

func TestCalculateSizesInitializesCurrentJournalOnlyOnce(t *testing.T) {
	storageDir := t.TempDir()
	previousFilePrefix := viper.Get("WALG_FILE_PREFIX")
	previousStoragePrefix := viper.Get(conf.StoragePrefixSetting)
	viper.Set("WALG_FILE_PREFIX", storageDir)
	viper.Set(conf.StoragePrefixSetting, "")
	t.Cleanup(func() {
		viper.Set("WALG_FILE_PREFIX", previousFilePrefix)
		viper.Set(conf.StoragePrefixSetting, previousStoragePrefix)
	})

	ctx := t.Context()
	storage, err := internal.ConfigureStorage(ctx)
	require.NoError(t, err)
	rootFolder := storage.RootFolder()
	backupFolder := rootFolder.GetSubFolder(utility.BaseBackupPath)
	oplogFolder := rootFolder.GetSubFolder(models.OplogArchBasePath)

	previousBackupName := "binary_20261003T222334Z"
	currentBackupName := "binary_20261004T221339Z"
	previousBackupEnd := time.Date(2026, time.October, 3, 22, 23, 34, 0, time.UTC)
	oplogTime := previousBackupEnd.Add(time.Hour)
	currentBackupEnd := previousBackupEnd.Add(2 * time.Hour)
	oplogData := []byte("oplog data")

	previousSentinelName := internal.SentinelNameFromBackup(previousBackupName)
	currentSentinelName := internal.SentinelNameFromBackup(currentBackupName)
	require.NoError(t, backupFolder.PutObject(ctx, previousSentinelName, bytes.NewReader(nil)))
	require.NoError(t, backupFolder.PutObject(ctx, currentSentinelName, bytes.NewReader(nil)))
	require.NoError(t, oplogFolder.PutObject(ctx, "oplog.br", bytes.NewReader(oplogData)))

	require.NoError(t, os.Chtimes(
		filepath.Join(storageDir, utility.BaseBackupPath, previousSentinelName),
		previousBackupEnd,
		previousBackupEnd,
	))
	require.NoError(t, os.Chtimes(
		filepath.Join(storageDir, utility.BaseBackupPath, currentSentinelName),
		currentBackupEnd,
		currentBackupEnd,
	))
	require.NoError(t, os.Chtimes(
		filepath.Join(storageDir, models.OplogArchBasePath, "oplog.br"),
		oplogTime,
		oplogTime,
	))

	backupService := &BackupService{}
	backupService.calculateSizes(ctx, CalculateSizesArgs{
		BackupName:    currentBackupName,
		CountJournals: true,
	})

	objects, _, err := backupFolder.ListFolder(ctx)
	require.NoError(t, err)
	journalCount := 0
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), internal.JournalPrefix) {
			journalCount++
		}
	}
	require.Equal(t, 2, journalCount)

	previousJournal, err := internal.NewJournalInfo(ctx, previousBackupName, rootFolder, models.OplogArchBasePath)
	require.NoError(t, err)
	currentJournal, err := internal.NewJournalInfo(ctx, currentBackupName, rootFolder, models.OplogArchBasePath)
	require.NoError(t, err)

	require.Equal(t, int64(len(oplogData)), previousJournal.SizeToNextBackup)
	require.WithinDuration(t, previousBackupEnd, currentJournal.PriorBackupEnd, 0)
	require.WithinDuration(t, currentBackupEnd, currentJournal.CurrentBackupEnd, 0)
}
