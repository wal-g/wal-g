package etcd_test

import (
	"context"
	"encoding/json"
	"os"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/etcd"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/testtools"
	mock_internal "github.com/wal-g/wal-g/testtools/mocks"
	"go.uber.org/mock/gomock"
)

func init() {
	internal.ConfigureSettings("")
	config.InitConfig()
	config.Configure()
}

func TestFetch(t *testing.T) {
	folder := testtools.CreateMockStorageFolder(t.Context())
	backupName := "test"
	data := "Data"
	date := time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC)

	testObject := etcd.StreamSentinelDto{
		StartLocalTime: date,
		IsPermanent:    false,
		UserData:       data,
	}

	var expectedResult = internal.GenericMetadata{
		BackupName:  backupName,
		StartTime:   date,
		IsPermanent: false,
		UserData:    data,
	}

	_ = internal.UploadDto(t.Context(), folder, testObject, internal.SentinelNameFromBackup(backupName))

	actualResult, err := etcd.NewGenericMetaFetcher().Fetch(t.Context(), backupName, folder)
	assert.NoError(t, err)

	isEqualTimeStart := expectedResult.StartTime.Equal(actualResult.StartTime)
	assert.True(t, isEqualTimeStart)

	expectedResult.StartTime = actualResult.StartTime
	expectedResult.FinishTime = actualResult.FinishTime

	assert.NoError(t, err)
	assert.Equal(t, expectedResult, actualResult)
}

func TestWALPushArchivesAfterCachedWalWithHighSequence(t *testing.T) {
	// 0x1e0 is the only file after the cached 0x1df that is not the newest one, so it is the only upload.
	testWALPushFromCache(t, 2, []int{3})
}

func TestWALPushClearsCacheWithBadWalName(t *testing.T) {
	// A cached name that is not a wal file clears the cache: every file but the newest is uploaded.
	testWALPushFromCache(t, -1, []int{0, 1, 2, 3})
}

func testWALPushFromCache(t *testing.T, cachedIdx int, wantIdx []int) {
	// The WAL cache lives in the current user's home dir: keep its old content and restore it afterwards.
	usr, err := user.Current()
	assert.NoError(t, err)
	cachePath := filepath.Join(usr.HomeDir, ".walg_etcd_wals_cache")
	prevCache, prevErr := os.ReadFile(cachePath)
	if prevErr != nil && !os.IsNotExist(prevErr) {
		t.Skipf("cannot read the existing WAL cache %s, not touching it: %v", cachePath, prevErr)
	}
	t.Cleanup(func() {
		if os.IsNotExist(prevErr) {
			_ = os.Remove(cachePath)
		} else {
			_ = os.WriteFile(cachePath, prevCache, 0644)
		}
	})

	// Sequence numbers 0x1dd..0x1e1 are above the number of local files; the old code uploaded nothing here.
	walDir := t.TempDir()
	names := []string{
		"00000000000001dd-00000000000003e8.wal",
		"00000000000001de-00000000000003e8.wal",
		"00000000000001df-00000000000003e8.wal",
		"00000000000001e0-00000000000003e8.wal",
		"00000000000001e1-00000000000003e8.wal",
	}
	for _, name := range names {
		assert.NoError(t, os.WriteFile(filepath.Join(walDir, name), nil, 0644))
	}
	cached := "bogus"
	if cachedIdx >= 0 {
		cached = names[cachedIdx]
	}
	cacheJSON, err := json.Marshal(etcd.LogsCache{LastArchivedWal: cached})
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(cachePath, cacheJSON, 0644))

	viper.Set(config.ETCDWalDirectory, walDir)
	t.Cleanup(func() { viper.Set(config.ETCDWalDirectory, nil) })

	var uploaded []string
	ctrl := gomock.NewController(t)
	uploader := mock_internal.NewMockUploader(ctrl)
	uploader.EXPECT().ChangeDirectory(gomock.Any()).AnyTimes()
	uploader.EXPECT().UploadFile(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, file ioextensions.NamedReader) error {
			uploaded = append(uploaded, filepath.Base(file.Name()))
			return nil
		}).AnyTimes()

	assert.NoError(t, etcd.HandleWALPush(t.Context(), uploader, t.TempDir()))
	var want []string
	for _, i := range wantIdx {
		want = append(want, names[i])
	}
	assert.Equal(t, want, uploaded)
}
