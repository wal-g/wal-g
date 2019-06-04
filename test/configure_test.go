package test

import (
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
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
