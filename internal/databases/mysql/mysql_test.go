package mysql

import (
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"os"
	"testing"
)

func TestGetBinlogConfig(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mock := testtools.NewMockLogFetchSettings(mockCtrl)
	mock.EXPECT().GetDstEnv().Return(BinlogDstSetting).AnyTimes()
	mock.EXPECT().GetEndTsEnv().Return(BinlogEndTsSetting).AnyTimes()

	viper.AutomaticEnv()
	os.Setenv(BinlogEndTsSetting, "2018-12-06T11:50:58Z")
	samplePath := "/xxx/"
	os.Setenv(BinlogDstSetting, samplePath)
	time, path, err := internal.GetOperationLogsSettings(mock)
	assert.NoError(t, err)
	assert.Equal(t, (*time).Year(), 2018)
	assert.Equal(t, int((*time).Month()), 12)
	assert.Equal(t, (*time).Day(), 6)
	assert.Equal(t, path, samplePath)
	os.Unsetenv(BinlogEndTsSetting)
	os.Unsetenv(BinlogDstSetting)
}

func TestGetBinlogConfigNoError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mock := testtools.NewMockLogFetchSettings(mockCtrl)
	mock.EXPECT().GetDstEnv().Return(BinlogDstSetting).AnyTimes()
	mock.EXPECT().GetEndTsEnv().Return(BinlogEndTsSetting).AnyTimes()

	os.Unsetenv(BinlogEndTsSetting)
	os.Unsetenv(BinlogDstSetting)
	_, _, err := internal.GetOperationLogsSettings(mock)
	assert.Error(t, err)
	assert.IsType(t, internal.UnsetRequiredSettingError{}, err)
}
