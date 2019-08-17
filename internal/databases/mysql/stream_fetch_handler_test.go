package mysql

import (
	"bytes"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/storages/memory"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
	"strings"
	"testing"
	"time"
)

func TestFetchBinlogs(t *testing.T) {
	storage_, cutPoint := fillTestStorage()

	folder := memory.NewFolder("", storage_)
	objects, _, err := folder.ListFolder()

	var startBinlog storage.Object
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), "mysql-bin-log.000018.lz4") {
			startBinlog = object
		}
	}

	assert.NotNil(t, startBinlog)
	assert.NoError(t, err)
	assert.Equal(t, len(objects), 4)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	allowed := []string{"mysql-bin-log.000018", "mysql-bin-log.000019"}

	for _, object := range objects {
		binlogName := utility.TrimFileExtension(object.GetName())
		data, exist := storage_.Load(object.GetName())

		assert.True(t, exist)

		timestamp, err := parseFirstTimestampFromHeader(getTestReadSeekCloser(mockCtrl, data.Data))

		assert.NoError(t, err)
		if internal.LogFileShouldBeFetched(startBinlog.GetLastModified(), nil, object) && !binlogIsTooOld(time.Unix(int64(timestamp), 0), &cutPoint) {
			assert.Contains(t, allowed, binlogName)
		} else {
			assert.NotContains(t, allowed, binlogName)
		}
	}
}

func fillTestStorage() (*memory.Storage, time.Time) {
	storage_ := memory.NewStorage()
	storage_.Store("mysql-bin-log.000017.lz4", *bytes.NewBuffer([]byte{0x01, 0x00, 0x00, 0x00}))
	storage_.Store("mysql-bin-log.000018.lz4", *bytes.NewBuffer([]byte{0x02, 0x00, 0x00, 0x00}))
	cutPoint := utility.TimeNowCrossPlatformUTC()
	time.Sleep(time.Millisecond * 20)
	storage_.Store("mysql-bin-log.000019.lz4", *bytes.NewBuffer([]byte{0x03, 0x00, 0x00, 0x00}))
	time.Sleep(time.Millisecond * 20)

	// we will parse 2 ** 31 - 1 from header - binlog will be too old
	storage_.Store("mysql-bin-log.000020.lz4", *bytes.NewBuffer([]byte{0xFF, 0xFF, 0xFF, 0x7F}))

	return storage_, cutPoint
}

func getTestReadSeekCloser(mockCtrl *gomock.Controller, data bytes.Buffer) ioextensions.ReadSeekCloser {
	testFileReadSeekCloser := testtools.NewMockReadSeekCloser(mockCtrl)

	testFileReadSeekCloser.EXPECT().Read(gomock.Any()).Do(func(p []byte) {
		_, _ = data.Read(p)
	})
	testFileReadSeekCloser.EXPECT().Seek(gomock.Any(), gomock.Any()).Times(1)
	testFileReadSeekCloser.EXPECT().Close().Times(1)

	return testFileReadSeekCloser
}
