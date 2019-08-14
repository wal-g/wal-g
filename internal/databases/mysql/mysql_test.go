package mysql

import (
	"bytes"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/memory"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGetBinlogConfig(t *testing.T) {
	viper.AutomaticEnv()
	os.Setenv(BinlogEndTsSetting, "2018-12-06T11:50:58Z")
	samplePath := "/xxx/"
	os.Setenv(BinlogDstSetting, samplePath)
	time, path, err := GetBinlogConfigs()
	assert.NoError(t, err)
	assert.Equal(t, (*time).Year(), 2018)
	assert.Equal(t, int((*time).Month()), 12)
	assert.Equal(t, (*time).Day(), 6)
	assert.Equal(t, path, samplePath)
	os.Unsetenv(BinlogEndTsSetting)
	os.Unsetenv(BinlogDstSetting)
}

func TestGetBinlogConfigNoError(t *testing.T) {
	os.Unsetenv(BinlogEndTsSetting)
	os.Unsetenv(BinlogDstSetting)
	_, _, err := GetBinlogConfigs()
	assert.Error(t, err)
	assert.IsType(t, internal.UnsetRequiredSettingError{}, err)
}

func TestBinlogShouldBeFetched(t *testing.T) {
	storage_ := memory.NewStorage()
	storage_.Store("mysql-bin-log.000017.lz4", *bytes.NewBuffer([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}))
	storage_.Store("mysql-bin-log.000018.lz4", *bytes.NewBuffer([]byte{0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}))
	cutpoint := utility.TimeNowCrossPlatformLocal()

	time.Sleep(time.Millisecond * 20)

	storage_.Store("mysql-bin-log.000019.lz4", *bytes.NewBuffer([]byte{0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00}))

	time.Sleep(time.Millisecond * 20)

	// we will parse 2 ** 31 - 1 from header - binlog will be too old
	storage_.Store("mysql-bin-log.000020.lz4", *bytes.NewBuffer([]byte{0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0x7F}))

	folder := memory.NewFolder("", storage_)
	objects, _, err := folder.ListFolder()

	var startBinlog storage.Object
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), "mysql-bin-log.000018.lz4") {
			startBinlog = object
		}
	}

	assert.NoError(t, err)
	assert.Equal(t, 4, len(objects))

	for _, object := range objects {
		binlogName := ExtractBinlogName(object, folder) + ".lz4"

		assert.NoError(t, err)

		data, exist := storage_.Load(binlogName)

		assert.True(t, exist)

		testFileReadSeekCloser := &testtools.MockReadSeekCloser{Testdata: data.Data.Bytes()}
		tm, err := parseFirstTimestampFromHeader(testFileReadSeekCloser)

		assert.NoError(t, err)

		fetched := BinlogShouldBeFetched(startBinlog.GetLastModified(), object) && !BinlogIsTooOld(time.Unix(int64(tm), 0), &cutpoint)
		if fetched {
			allowed := []string{"mysql-bin-log.000018.lz4", "mysql-bin-log.000019.lz4"}
			assert.Contains(t, allowed, binlogName)
		} else {
			allowed := []string{"mysql-bin-log.000017.lz4", "mysql-bin-log.000020.lz4"}
			assert.Contains(t, allowed, binlogName)
		}
	}
}
