package mysql

import (
	"bytes"
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/storages/memory"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type TestBinlogHandlers struct {
	readSeekCloser ioextensions.ReadSeekCloser
	endTS          *time.Time
}

func (t TestBinlogHandlers) HandleAbortFetch(string) error {
	return nil
}

func (t TestBinlogHandlers) GetLogFilePath(pathToLog string) (string, error) {
	return "", nil
}

func (t TestBinlogHandlers) ShouldBeAborted(pathToLog string) (bool, error) {
	tm, _ := parseFirstTimestampFromHeader(t.readSeekCloser)

	return binlogIsTooOld(time.Unix(int64(tm), 0), t.endTS), nil
}

func (t TestBinlogHandlers) DownloadLogTo(logFolder storage.Folder, logName string, dstLogFilePath string) error {
	return nil
}

func TestFetchBinlogs(t *testing.T) {
	storage_, cutPoint := fillTestStorage()

	folder := memory.NewFolder("", storage_)
	objects, _, err := folder.GetSubFolder(BinlogPath).ListFolder()

	var startBinlog storage.Object
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), "mysql-bin-log.000018.lz4") {
			startBinlog = object
		}
	}

	assert.NotNil(t, startBinlog)
	assert.NoError(t, err)
	assert.Equal(t, len(objects), 4)

	allowed := []string{"mysql-bin-log.000018", "mysql-bin-log.000019"}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	headersData := make([]bytes.Buffer, 0)

	for _, object := range objects {
		data, exist := storage_.Load(filepath.Join(BinlogPath, object.GetName()))
		assert.True(t, exist)

		if object.GetName() == "mysql-bin-log.000017" {
			continue
		}
		headersData = append(headersData, data.Data)
	}

	viper.AutomaticEnv()
	os.Setenv(BinlogEndTsSetting, cutPoint.Format("2006-01-02T15:04:05Z07:00"))
	samplePath := "/xxx/"
	os.Setenv(BinlogDstSetting, samplePath)

	var settings BinlogFetchSettings
	var handlers TestBinlogHandlers

	handlers.readSeekCloser = getTestReadSeekCloserWithExpectedData(mockController, headersData)
	handlers.endTS = &cutPoint

	fetched, err := internal.FetchLogs(folder, startBinlog.GetLastModified(), nil, settings, handlers)
	assert.NoError(t, err)

	for _, object := range fetched {
		binlogName := utility.TrimFileExtension(object.GetName())
		assert.Contains(t, allowed, binlogName)
	}

	os.Unsetenv(BinlogEndTsSetting)
	os.Unsetenv(BinlogDstSetting)
}

func fillTestStorage() (*memory.Storage, time.Time) {
	storage_ := memory.NewStorage()
	storage_.Store(filepath.Join(BinlogPath, "mysql-bin-log.000017.lz4"), *bytes.NewBuffer([]byte{0x01, 0x00, 0x00, 0x00}))
	storage_.Store(filepath.Join(BinlogPath, "mysql-bin-log.000018.lz4"), *bytes.NewBuffer([]byte{0x02, 0x00, 0x00, 0x00}))
	cutPoint := utility.TimeNowCrossPlatformUTC()
	time.Sleep(time.Millisecond * 20)
	// this binlog will be uploaded to storage too late (in terms of GetLastModified() func)
	storage_.Store(filepath.Join(BinlogPath, "mysql-bin-log.000019.lz4"), *bytes.NewBuffer([]byte{0x03, 0x00, 0x00, 0x00}))
	time.Sleep(time.Millisecond * 20)

	// we will parse 2 ** 31 - 1 from header - binlog will be too old
	storage_.Store(filepath.Join(BinlogPath, "mysql-bin-log.000020.lz4"), *bytes.NewBuffer([]byte{0xFF, 0xFF, 0xFF, 0x7F}))

	return storage_, cutPoint
}

func getTestReadSeekCloserWithExpectedData(mockCtrl *gomock.Controller, headersData []bytes.Buffer) ioextensions.ReadSeekCloser {
	testFileReadSeekCloser := testtools.NewMockReadSeekCloser(mockCtrl)

	testFileReadSeekCloser.EXPECT().Read(gomock.Any()).Do(func(p []byte) {

		data := headersData[0]
		headersData = headersData[1:]

		_, _ = data.Read(p)
	}).AnyTimes()

	testFileReadSeekCloser.EXPECT().Close().AnyTimes()
	testFileReadSeekCloser.EXPECT().Seek(gomock.Any(), gomock.Any()).AnyTimes()

	return testFileReadSeekCloser
}
