package config_test

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
)

func TestMysqlFailoverStoragesCheckTimeout_IsAllowed(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	_ = os.Setenv("WALG_FAILOVER_STORAGES_CHECK_TIMEOUT", "3600")
	defer func() { _ = os.Unsetenv("WALG_FAILOVER_STORAGES_CHECK_TIMEOUT") }()

	viper.Set(config.FailoverStoragesCheckTimeout, "3600")

	allowed := config.AllowedSettings[config.FailoverStoragesCheckTimeout]
	assert.True(t, allowed)

	value := viper.GetString(config.FailoverStoragesCheckTimeout)
	assert.Equal(t, "3600", value)

	viper.Reset()
}

func TestMysqlFailoverStoragesCacheLifetime_IsAllowed(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	_ = os.Setenv("WALG_FAILOVER_STORAGES_CACHE_LIFETIME", "3600")
	defer func() { _ = os.Unsetenv("WALG_FAILOVER_STORAGES_CACHE_LIFETIME") }()

	viper.Set(config.FailoverStorageCacheLifetime, "3600")

	allowed := config.AllowedSettings[config.FailoverStorageCacheLifetime]
	assert.True(t, allowed)

	value := viper.GetString(config.FailoverStorageCacheLifetime)
	assert.Equal(t, "3600", value)

	viper.Reset()
}

func TestMysqlFailoverStorages_IsAllowed(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	viper.Set(config.FailoverStorages, "storage1,storage2")

	allowed := config.AllowedSettings[config.FailoverStorages]
	assert.True(t, allowed)

	viper.Reset()
}

func TestMysqlFailoverStoragesCheck_IsAllowed(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	viper.Set(config.FailoverStoragesCheck, "true")

	allowed := config.AllowedSettings[config.FailoverStoragesCheck]
	assert.True(t, allowed)

	viper.Reset()
}

func TestMysqlFailoverStoragesCheckSize_IsAllowed(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	viper.Set(config.FailoverStoragesCheckSize, "1mb")

	allowed := config.AllowedSettings[config.FailoverStoragesCheckSize]
	assert.True(t, allowed)

	viper.Reset()
}

func TestMysqlFailoverStorages_DefaultValues(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	assert.Contains(t, config.DefaultConfigValues, config.FailoverStoragesCheckTimeout)
	assert.Contains(t, config.DefaultConfigValues, config.FailoverStorageCacheLifetime)

	checkTimeout := config.DefaultConfigValues[config.FailoverStoragesCheckTimeout]
	assert.Equal(t, "30s", checkTimeout)

	cacheLifetime := config.DefaultConfigValues[config.FailoverStorageCacheLifetime]
	assert.Equal(t, "15m", cacheLifetime)

	viper.Reset()
}

func TestMysqlFailoverStoragesEMA_AllSettingsAllowed(t *testing.T) {
	viper.Reset()
	internal.ConfigureSettings(config.MYSQL)
	config.InitConfig()

	emaSettings := []string{
		config.FailoverStorageCacheEMAAliveLimit,
		config.FailoverStorageCacheEMADeadLimit,
		config.FailoverStorageCacheEMAAlphaAliveMax,
		config.FailoverStorageCacheEMAAlphaAliveMin,
		config.FailoverStorageCacheEMAAlphaDeadMax,
		config.FailoverStorageCacheEMAAlphaDeadMin,
	}

	for _, setting := range emaSettings {
		allowed := config.AllowedSettings[setting]
		assert.True(t, allowed, setting)
	}

	viper.Reset()
}
