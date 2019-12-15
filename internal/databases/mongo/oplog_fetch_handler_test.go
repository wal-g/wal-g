package mongo

import (
	"bytes"
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/memory"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	mock_internal "github.com/wal-g/wal-g/internal/databases/testtools"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestFetchOplogs(t *testing.T) {
	storage_, cutPoint := fillTestStorage(OplogPath)
	folder := memory.NewFolder("", storage_)
	objects, _, err := folder.GetSubFolder(OplogPath).ListFolder()

	var startBinlog storage.Object
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), "oplog.000001.lz4") {
			startBinlog = object
		}
	}

	assert.NotNil(t, startBinlog)
	assert.NoError(t, err)
	assert.Equal(t, len(objects), 4)

	allowed := []string{"oplog.000001", "oplog.000002"}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	headersData := make([]bytes.Buffer, 0)

	for _, object := range objects {
		data, exist := storage_.Load(filepath.Join(OplogPath, object.GetName()))
		assert.True(t, exist)
		headersData = append(headersData, data.Data)
	}

	sort.Slice(headersData, func(i, j int) bool {
		return objects[i].GetLastModified().Before(objects[j].GetLastModified())
	})

	viper.AutomaticEnv()
	os.Setenv(OplogEndTs, cutPoint.Format("2006-01-02T15:04:05Z07:00"))
	samplePath := "/xxx/"
	os.Setenv(OplogDst, samplePath)

	settings := mock_internal.NewMockLogFetchSettings(gomock.NewController(t))
	settings.EXPECT().GetEndTS().Return(&cutPoint, nil)
	settings.EXPECT().GetDestFolderPath().Return(internal.GetLogsDstSettings(OplogDst)).AnyTimes()
	settings.EXPECT().GetLogFolderPath().Return(OplogPath).AnyTimes()

	handlers := mock_internal.NewMockLogFetchHandlers(gomock.NewController(t))
	handlers.EXPECT().GetLogFilePath(gomock.Any()).Times(4)
	handlers.EXPECT().DownloadLogTo(gomock.Any(), gomock.Any(), gomock.Any()).Times(3)
	handlers.EXPECT().ShouldBeAborted(gomock.Any()).Times(3)

	fetched, err := internal.FetchLogs(folder, startBinlog.GetLastModified(), &cutPoint, settings.GetLogFolderPath(), handlers)
	assert.NoError(t, err)

	for _, object := range fetched {
		binlogName := utility.TrimFileExtension(object.GetName())
		assert.Contains(t, allowed, binlogName)
	}

	os.Unsetenv(OplogEndTs)
	os.Unsetenv(OplogDst)
}

func fillTestStorage(path string) (*memory.Storage, time.Time) {
	storage_ := memory.NewStorage()
	storage_.Store(filepath.Join(path, "oplog.000000.lz4"), *bytes.NewBuffer([]byte{0x01, 0x00, 0x00, 0x00}))
	storage_.Store(filepath.Join(path, "oplog.000001.lz4"), *bytes.NewBuffer([]byte{0x02, 0x00, 0x00, 0x00}))
	cutPoint := utility.TimeNowCrossPlatformUTC()
	time.Sleep(time.Millisecond * 20)
	// those oplog was uploadede too late
	storage_.Store(filepath.Join(path, "oplog.000002.lz4"), *bytes.NewBuffer([]byte{0x03, 0x00, 0x00, 0x00}))
	storage_.Store(filepath.Join(path, "oplog.000003.lz4"), *bytes.NewBuffer([]byte{0xFF, 0xFF, 0xFF, 0x7F}))

	return storage_, cutPoint
}
