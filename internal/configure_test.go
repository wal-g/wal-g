package internal_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func TestGetMaxConcurrency_InvalidKey(t *testing.T) {
	_, err := internal.GetMaxConcurrency("INVALID_KEY")

	assert.Error(t, err)
}

func TestGetMaxConcurrency_ValidKey(t *testing.T) {
	viper.Set(internal.UploadConcurrencySetting, "100")
	actual, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.NoError(t, err)
	assert.Equal(t, 100, actual)
}

func TestGetMaxConcurrency_ValidKeyAndNegativeValue(t *testing.T) {
	viper.Set(internal.UploadConcurrencySetting, "-5")
	_, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.Error(t, err)
}

func TestGetMaxConcurrency_ValidKeyAndInvalidValue(t *testing.T) {
	viper.Set(internal.UploadConcurrencySetting, "invalid")
	_, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.Error(t, err)
}

func TestGetSentinelUserData(t *testing.T) {

	viper.Set(internal.SentinelUserDataSetting, "1.0")

	data := internal.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, 1.0, data.(float64), "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(internal.SentinelUserDataSetting, "\"1\"")

	data = internal.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, "1", data.(string), "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(internal.SentinelUserDataSetting, `{"x":123,"y":["asdasd",123]}`)

	data = internal.GetSentinelUserData()
	t.Log(data)
	assert.NotNilf(t, data, "Unable to parse WALG_SENTINEL_USER_DATA")
}

func TestGetDataFolderPath_Default(t *testing.T) {
	viper.Set(internal.PgDataSetting, nil)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(internal.DefaultDataFolderPath, "walg_data"), actual)
}

func TestGetDataFolderPath_FolderNotExist(t *testing.T) {
	parentDir := prepareDataFolder(t, "someOtherFolder")
	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(internal.DefaultDataFolderPath, "walg_data"), actual)
	cleanup(t, parentDir)
}

func TestGetDataFolderPath_Wal(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_wal")

	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(parentDir, "pg_wal", "walg_data"), actual)
	cleanup(t, parentDir)
}

func TestGetDataFolderPath_Xlog(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_xlog")

	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(parentDir, "pg_xlog", "walg_data"), actual)
	cleanup(t, parentDir)
}

func TestGetDataFolderPath_WalIgnoreXlog(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_xlog")
	err := os.Mkdir(filepath.Join(parentDir, "pg_wal"), 0700)
	if err != nil {
		t.Log(err)
	}
	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(parentDir, "pg_wal", "walg_data"), actual)
	cleanup(t, parentDir)
}

func TestConfigureLogging_WhenLogLevelSettingIsNotSet(t *testing.T) {
	assert.NoError(t, internal.ConfigureLogging())
}

func TestConfigureLogging_WhenLogLevelSettingIsSet(t *testing.T) {
	parentDir := prepareDataFolder(t, "someOtherFolder")
	viper.Set(internal.LogLevelSetting, parentDir)
	err := internal.ConfigureLogging()

	assert.Error(t, tracelog.UpdateLogLevel(viper.GetString(internal.LogLevelSetting)), err)
}

func prepareDataFolder(t *testing.T, name string) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	// Create temp directory.
	dir, err := ioutil.TempDir(cwd, "test")
	if err != nil {
		t.Log(err)
	}
	err = os.Mkdir(filepath.Join(dir, name), 0700)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(dir)
	return dir
}
