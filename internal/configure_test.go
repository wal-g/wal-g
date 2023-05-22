package internal_test

import (
	"fmt"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"os"
	"path/filepath"
	"testing"

	"github.com/wal-g/wal-g/testtools"

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
	resetToDefaults()
}

func TestGetMaxConcurrency_ValidKeyAndNegativeValue(t *testing.T) {
	viper.Set(internal.UploadConcurrencySetting, "-5")
	_, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.Error(t, err)
	resetToDefaults()
}

func TestGetMaxConcurrency_ValidKeyAndInvalidValue(t *testing.T) {
	viper.Set(internal.UploadConcurrencySetting, "invalid")
	_, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.Error(t, err)
	resetToDefaults()
}

func TestGetSentinelUserData(t *testing.T) {
	viper.Set(internal.SentinelUserDataSetting, "1.0")

	data, err := internal.GetSentinelUserData()
	assert.NoError(t, err)
	t.Log(data)
	assert.Equalf(t, 1.0, data.(float64), "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(internal.SentinelUserDataSetting, "\"1\"")

	data, err = internal.GetSentinelUserData()
	assert.NoError(t, err)
	t.Log(data)
	assert.Equalf(t, "1", data.(string), "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(internal.SentinelUserDataSetting, `{"x":123,"y":["asdasd",123]}`)

	data, err = internal.GetSentinelUserData()
	assert.NoError(t, err)
	t.Log(data)
	assert.NotNilf(t, data, "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(internal.SentinelUserDataSetting, `"x",1`)

	data, err = internal.GetSentinelUserData()
	assert.Error(t, err, "Should fail on the invalid user data")
	t.Log(err)
	assert.Nil(t, data)
	resetToDefaults()
}

func TestGetDataFolderPath_Default(t *testing.T) {
	viper.Set(internal.PgDataSetting, nil)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(internal.DefaultDataFolderPath, "walg_data"), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_FolderNotExist(t *testing.T) {
	parentDir := prepareDataFolder(t, "someOtherFolder")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(internal.DefaultDataFolderPath, "walg_data"), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_Wal(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_wal")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(parentDir, "pg_wal", "walg_data"), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_Xlog(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_xlog")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(parentDir, "pg_xlog", "walg_data"), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_WalIgnoreXlog(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_xlog")
	defer testtools.Cleanup(t, parentDir)

	err := os.Mkdir(filepath.Join(parentDir, "pg_wal"), 0700)
	if err != nil {
		t.Log(err)
	}
	viper.Set(internal.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.Join(parentDir, "pg_wal", "walg_data"), actual)
	resetToDefaults()
}

func TestConfigureLogging_WhenLogLevelSettingIsNotSet(t *testing.T) {
	assert.NoError(t, internal.ConfigureLogging())
}

func TestConfigureLogging_WhenLogLevelSettingIsSet(t *testing.T) {
	parentDir := prepareDataFolder(t, "someOtherFolder")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(internal.LogLevelSetting, parentDir)
	err := internal.ConfigureLogging()

	assert.Error(t, tracelog.UpdateLogLevel(viper.GetString(internal.LogLevelSetting)), err)
	resetToDefaults()
}

func TestConfigureCompressor_Lz4Method(t *testing.T) {
	viper.Set(internal.CompressionMethodSetting, "lz4")
	compressor, err := internal.ConfigureCompressor()
	assert.NoError(t, err)
	assert.Equal(t, compressor, lz4.Compressor{})
	resetToDefaults()
}

func TestConfigureCompressor_LzmaMethod(t *testing.T) {
	viper.Set(internal.CompressionMethodSetting, "lzma")
	compressor, err := internal.ConfigureCompressor()
	assert.NoError(t, err)
	assert.Equal(t, compressor, lzma.Compressor{})
	resetToDefaults()
}

func TestConfigureCompressor_UseDefaultOnNoMethodSet(t *testing.T) {
	compressor, err := internal.ConfigureCompressor()
	assert.NoError(t, err)
	assert.Equal(t, compressor, lz4.Compressor{})
	resetToDefaults()
}

func TestConfigureCompressor_ErrorWhenViperClear(t *testing.T) {
	viper.Reset()
	compressor, err := internal.ConfigureCompressor()
	assert.Error(t, err)
	assert.Equal(t, compressor, nil)
	resetToDefaults()
}

func TestConfigureCompressor_FailsOnInvalidCompressorString(t *testing.T) {
	viper.Set(internal.CompressionMethodSetting, "kek123kek")
	compressor, err := internal.ConfigureCompressor()
	assert.Error(t, err)
	assert.Equal(t, compressor, nil)
	resetToDefaults()
}

func prepareDataFolder(t *testing.T, name string) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	// Create temp directory.
	dir, err := os.MkdirTemp(cwd, "test")
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

func resetToDefaults() {
	viper.Reset()
	internal.ConfigureSettings(internal.PG)
	internal.InitConfig()
	internal.Configure()
}
