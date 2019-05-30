package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/storages/memory"
	"github.com/wal-g/wal-g/utility"
	"os"
	"testing"
	"time"
)

func TestGetBinlogConfig(t *testing.T) {
	os.Setenv(mysql.BinlogEndTsSetting, "2018-12-06T11:50:58Z")
	samplePath := "/xxx/"
	os.Setenv(mysql.BinlogDstSetting, samplePath)
	time, path, err := mysql.GetBinlogConfigs()
	assert.NoError(t, err)
	assert.Equal(t, (*time).Year(), 2018)
	assert.Equal(t, int((*time).Month()), 12)
	assert.Equal(t, (*time).Day(), 6)
	assert.Equal(t, path, samplePath)
	os.Unsetenv(mysql.BinlogEndTsSetting)
	os.Unsetenv(mysql.BinlogDstSetting)
}

func TestGetBinlogConfigNoError(t *testing.T) {
	os.Unsetenv(mysql.BinlogEndTsSetting)
	os.Unsetenv(mysql.BinlogDstSetting)
	_, _, err := mysql.GetBinlogConfigs()
	assert.Error(t, err)
	assert.IsType(t, internal.UnsetRequiredSettingError{}, err)
}

func TestBinlogShouldBeFetched(t *testing.T) {
	storage := memory.NewStorage()
	storage.Store("mysql-bin-log.000017.lz4", *bytes.NewBufferString(""))
	storage.Store("mysql-bin-log.000018.lz4", *bytes.NewBufferString(""))
	storage.Store("mysql-bin-log.000019.lz4", *bytes.NewBufferString(""))
	time.Sleep(time.Millisecond * 20)
	cutpoint := utility.CeilTimeUpToMicroseconds(time.Now())
	time.Sleep(time.Millisecond * 20)
	storage.Store("mysql-bin-log.000020.lz4", *bytes.NewBufferString(""))

	folder := memory.NewFolder("", storage)
	objects, _, err := folder.ListFolder()

	dto := mysql.StreamSentinelDto{BinLogStart: "mysql-bin-log.000018", BinLogEnd: "mysql-bin-log.000019.lz4"}

	assert.NoError(t, err)
	assert.Equal(t, len(objects), 4)
	for _, o := range objects {
		binlogName := mysql.ExtractBinlogName(o, folder)
		fetched := mysql.BinlogShouldBeFetched(dto, binlogName, &cutpoint, o)
		if fetched {
			allowed := []string{"mysql-bin-log.000018", "mysql-bin-log.000019"}
			assert.Contains(t, allowed, binlogName)
		} else {
			allowed := []string{"mysql-bin-log.000017", "mysql-bin-log.000020"}
			assert.Contains(t, allowed, binlogName)
		}
	}
}
