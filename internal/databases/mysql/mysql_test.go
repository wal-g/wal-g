package mysql_test

import (
	"bytes"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/storages/memory"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGetBinlogConfig(t *testing.T) {
	viper.AutomaticEnv()
	os.Setenv(mysql.BinlogEndTsSetting, "2018-12-06T11:50:58Z")
	samplePath := "/xxx/"
	os.Setenv(mysql.BinlogDstSetting, samplePath)
	time, path, err := internal.GetOperationLogsSettings(mysql.BinlogEndTsSetting, mysql.BinlogDstSetting)
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
	_, _, err := internal.GetOperationLogsSettings(mysql.BinlogEndTsSetting, mysql.BinlogDstSetting)
	assert.Error(t, err)
	assert.IsType(t, internal.UnsetRequiredSettingError{}, err)
}

func TestBinlogShouldBeFetched(t *testing.T) {
	storage_ := memory.NewStorage()
	storage_.Store("mysql-bin-log.000017.lz4", *bytes.NewBufferString(""))
	storage_.Store("mysql-bin-log.000018.lz4", *bytes.NewBufferString(""))
	storage_.Store("mysql-bin-log.000019.lz4", *bytes.NewBufferString(""))
	time.Sleep(time.Millisecond * 20)
	cutpoint := utility.TimeNowCrossPlatformLocal()
	time.Sleep(time.Millisecond * 20)
	storage_.Store("mysql-bin-log.000020.lz4", *bytes.NewBufferString(""))

	folder := memory.NewFolder("", storage_)
	objects, _, err := folder.ListFolder()

	var startBinlog storage.Object
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), "mysql-bin-log.000018.lz4") {
			startBinlog = object
		}
	}

	assert.NoError(t, err)
	assert.Equal(t, len(objects), 4)
	for _, o := range objects {
		binlogName := utility.TrimFileExtension(o.GetName())
		fetched := internal.LogFileShouldBeFetched(startBinlog.GetLastModified(), &cutpoint, o)
		if fetched {
			allowed := []string{"mysql-bin-log.000018", "mysql-bin-log.000019"}
			assert.Contains(t, allowed, binlogName)
		} else {
			allowed := []string{"mysql-bin-log.000017", "mysql-bin-log.000020"}
			assert.Contains(t, allowed, binlogName)
		}
	}
}
