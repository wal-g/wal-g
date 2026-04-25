package internal_test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/limiters"
	"golang.org/x/time/rate"

	"github.com/wal-g/wal-g/testtools"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

func TestGetSentinelUserData(t *testing.T) {
	viper.Set(config.SentinelUserDataSetting, "1.0")

	data, err := internal.GetSentinelUserData()
	assert.NoError(t, err)
	t.Log(data)
	assert.Equalf(t, 1.0, data.(float64), "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(config.SentinelUserDataSetting, "\"1\"")

	data, err = internal.GetSentinelUserData()
	assert.NoError(t, err)
	t.Log(data)
	assert.Equalf(t, "1", data.(string), "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(config.SentinelUserDataSetting, `{"x":123,"y":["asdasd",123]}`)

	data, err = internal.GetSentinelUserData()
	assert.NoError(t, err)
	t.Log(data)
	assert.NotNilf(t, data, "Unable to parse WALG_SENTINEL_USER_DATA")

	viper.Set(config.SentinelUserDataSetting, `"x",1`)

	data, err = internal.GetSentinelUserData()
	assert.Error(t, err, "Should fail on the invalid user data")
	t.Log(err)
	assert.Nil(t, data)
	resetToDefaults()
}

func TestGetDataFolderPath_Default(t *testing.T) {
	pgEnv := os.Getenv(config.PgDataSetting)
	os.Unsetenv(config.PgDataSetting)
	// ensure the PgData environment variable is not set bc if it is set, viper returns it. viper.Set(..., nil) does not
	// "override" the environment variable. Likely a bug in viper: https://github.com/spf13/viper/blob/528f7416c4b56a4948673984b190bf8713f0c3c4/viper.go#L1212-L1216
	// some environments actually have PGDATA set _with_ proper structure (C:\PostgreSQL\17\data\pg_wal) and tests
	// fail because of that
	resetToDefaults()
	viper.Set(config.PgDataSetting, nil)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, path.Join(internal.GetDefaultDataFolderPath(), "walg_data"), actual)
	os.Setenv(config.PgDataSetting, pgEnv)
	resetToDefaults()
}

func TestGetDataFolderPath_FolderNotExist(t *testing.T) {
	parentDir := prepareDataFolder(t, "someOtherFolder")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(config.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, path.Join(internal.GetDefaultDataFolderPath(), "walg_data"), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_Wal(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_wal")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(config.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.ToSlash(filepath.Join(parentDir, "pg_wal", "walg_data")), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_Xlog(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_xlog")
	defer testtools.Cleanup(t, parentDir)

	viper.Set(config.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.ToSlash(filepath.Join(parentDir, "pg_xlog", "walg_data")), actual)
	resetToDefaults()
}

func TestGetDataFolderPath_WalIgnoreXlog(t *testing.T) {
	parentDir := prepareDataFolder(t, "pg_xlog")
	defer testtools.Cleanup(t, parentDir)

	err := os.Mkdir(filepath.Join(parentDir, "pg_wal"), 0700)
	if err != nil {
		t.Log(err)
	}
	viper.Set(config.PgDataSetting, parentDir)

	actual := internal.GetDataFolderPath()

	assert.Equal(t, filepath.ToSlash(filepath.Join(parentDir, "pg_wal", "walg_data")), actual)
	resetToDefaults()
}

func TestConfigureCompressor_Lz4Method(t *testing.T) {
	viper.Set(config.CompressionMethodSetting, "lz4")
	compressor, err := internal.ConfigureCompressor()
	assert.NoError(t, err)
	assert.Equal(t, compressor, lz4.Compressor{})
	resetToDefaults()
}

func TestConfigureCompressor_LzmaMethod(t *testing.T) {
	viper.Set(config.CompressionMethodSetting, "lzma")
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
	viper.Set(config.CompressionMethodSetting, "kek123kek")
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
	// Create temp Directory.
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
	internal.ConfigureSettings(config.PG)
	config.InitConfig()
	config.Configure()
}

func TestGetDeltaConfig(t *testing.T) {
	tests := []struct {
		name      string
		maxDeltas int
		origin    string
		wantMax   int
		wantFull  bool
	}{
		{
			name:      "latest origin",
			maxDeltas: 3,
			origin:    "LATEST",
			wantMax:   3,
			wantFull:  false,
		},
		{
			name:      "latest full origin",
			maxDeltas: 5,
			origin:    "LATEST_FULL",
			wantMax:   5,
			wantFull:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Set(config.DeltaMaxStepsSetting, tt.maxDeltas)
			viper.Set(config.DeltaOriginSetting, tt.origin)

			gotMax, gotFull := internal.GetDeltaConfig()

			assert.Equal(t, tt.wantMax, gotMax)
			assert.Equal(t, tt.wantFull, gotFull)

			resetToDefaults()
		})
	}
}

func TestGetDeltaConfig_DefaultOrigin(t *testing.T) {
	viper.Set(config.DeltaMaxStepsSetting, 7)

	gotMax, gotFull := internal.GetDeltaConfig()

	assert.Equal(t, 7, gotMax)
	assert.False(t, gotFull)

	resetToDefaults()
}

func TestConfigureLimiters_NoSettings(t *testing.T) {
	limiters.DiskLimiter = nil
	limiters.NetworkLimiter = nil
	defer func() {
		limiters.DiskLimiter = nil
		limiters.NetworkLimiter = nil
	}()

	internal.ConfigureLimiters()

	assert.Nil(t, limiters.DiskLimiter)
	assert.Nil(t, limiters.NetworkLimiter)
}

func TestConfigureLimiters_DiskRateLimit(t *testing.T) {
	limiters.DiskLimiter = nil
	defer func() { limiters.DiskLimiter = nil }()

	const diskLimit = int64(1024)
	viper.Set(config.DiskRateLimitSetting, diskLimit)
	defer resetToDefaults()

	internal.ConfigureLimiters()

	assert.NotNil(t, limiters.DiskLimiter)
	assert.Equal(t, rate.Limit(diskLimit), limiters.DiskLimiter.Limit())
	assert.Equal(t, int(diskLimit+internal.DefaultDataBurstRateLimit), limiters.DiskLimiter.Burst())
}

func TestConfigureLimiters_NetworkRateLimit(t *testing.T) {
	limiters.NetworkLimiter = nil
	defer func() { limiters.NetworkLimiter = nil }()

	const netLimit = int64(2048)
	viper.Set(config.NetworkRateLimitSetting, netLimit)
	defer resetToDefaults()

	internal.ConfigureLimiters()

	assert.NotNil(t, limiters.NetworkLimiter)
	assert.Equal(t, rate.Limit(netLimit), limiters.NetworkLimiter.Limit())
	assert.Equal(t, int(netLimit+internal.DefaultDataBurstRateLimit), limiters.NetworkLimiter.Burst())
}

func TestConfigureLimiters_BothLimits(t *testing.T) {
	limiters.DiskLimiter = nil
	limiters.NetworkLimiter = nil
	defer func() {
		limiters.DiskLimiter = nil
		limiters.NetworkLimiter = nil
	}()

	viper.Set(config.DiskRateLimitSetting, int64(512))
	viper.Set(config.NetworkRateLimitSetting, int64(1024))
	defer resetToDefaults()

	internal.ConfigureLimiters()

	assert.NotNil(t, limiters.DiskLimiter)
	assert.NotNil(t, limiters.NetworkLimiter)
}

func TestConfigureLimiters_TurboSkipsLimiters(t *testing.T) {
	limiters.DiskLimiter = nil
	limiters.NetworkLimiter = nil
	defer func() {
		limiters.DiskLimiter = nil
		limiters.NetworkLimiter = nil
		config.Turbo = false
	}()

	viper.Set(config.DiskRateLimitSetting, int64(1024))
	viper.Set(config.NetworkRateLimitSetting, int64(1024))
	config.Turbo = true
	defer resetToDefaults()

	internal.ConfigureLimiters()

	assert.Nil(t, limiters.DiskLimiter)
	assert.Nil(t, limiters.NetworkLimiter)
}

func TestConfigureStorage_NoStorageConfigured(t *testing.T) {
	resetToDefaults()

	st, err := internal.ConfigureStorage()

	assert.Error(t, err)
	assert.IsType(t, internal.UnconfiguredStorageError{}, err)
	assert.Nil(t, st)
}

func TestConfigureStorage_FileStorage(t *testing.T) {
	dir := t.TempDir()
	viper.Set("WALG_FILE_PREFIX", dir)
	defer resetToDefaults()

	st, err := internal.ConfigureStorage()

	assert.NoError(t, err)
	assert.NotNil(t, st)
	assert.NotEmpty(t, st.ConfigHash())
}

func TestConfigureStorage_FileStorageWithPrefix(t *testing.T) {
	dir := t.TempDir()
	viper.Set("WALG_FILE_PREFIX", dir)
	viper.Set(config.StoragePrefixSetting, "myprefix")
	defer resetToDefaults()

	st, err := internal.ConfigureStorage()

	assert.NoError(t, err)
	assert.NotNil(t, st)
	assert.Contains(t, st.RootFolder().GetPath(), "myprefix")
}

func TestConfigureStorage_WithNetworkLimiter(t *testing.T) {
	dir := t.TempDir()
	viper.Set("WALG_FILE_PREFIX", dir)
	limiters.NetworkLimiter = rate.NewLimiter(rate.Limit(1024), 1024)
	defer func() {
		limiters.NetworkLimiter = nil
		resetToDefaults()
	}()

	st, err := internal.ConfigureStorage()

	assert.NoError(t, err)
	assert.NotNil(t, st)
	_, isLimited := st.RootFolder().(*internal.LimitedFolder)
	assert.True(t, isLimited, "expected root folder to be wrapped in LimitedFolder")
}
