package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"os"
	"testing"
)

func TestConfigurePreventWalOverwrite_CorrectEnvVariable(t *testing.T) {
	os.Setenv(internal.PreventWalOverwriteSetting, "true")
	preventWalOverwrite, err := internal.ConfigurePreventWalOverwrite()
	assert.NoError(t, err)
	assert.Equal(t, true, preventWalOverwrite)
	os.Unsetenv(internal.PreventWalOverwriteSetting)
}

func TestConfigurePreventWalOverwrite_IncorrectEnvVariable(t *testing.T) {
	os.Setenv(internal.PreventWalOverwriteSetting, "fail")
	_, err := internal.ConfigurePreventWalOverwrite()
	assert.Error(t, err)
	os.Unsetenv(internal.PreventWalOverwriteSetting)
}

func TestGetMaxConcurrency_InvalidKey(t *testing.T) {
	_, err := internal.GetMaxConcurrency("INVALID_KEY")

	assert.Error(t, err)
}

func TestGetMaxConcurrency_ValidKey(t *testing.T) {
	os.Setenv(internal.UploadConcurrencySetting, "100")
	actual, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.NoError(t, err)
	assert.Equal(t, 100, actual)
	os.Unsetenv(internal.UploadConcurrencySetting)
}

func TestGetMaxConcurrency_ValidKeyAndInvalidDefaultValue(t *testing.T) {
	os.Setenv(internal.UploadConcurrencySetting, "100")
	actual, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.NoError(t, err)
	assert.Equal(t, 100, actual)
	os.Unsetenv(internal.UploadConcurrencySetting)
}

func TestGetMaxConcurrency_ValidKeyAndNegativeValue(t *testing.T) {
	os.Setenv(internal.UploadConcurrencySetting, "-5")
	_, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.Error(t, err)
	os.Unsetenv(internal.UploadConcurrencySetting)
}

func TestGetMaxConcurrency_ValidKeyAndInvalidValue(t *testing.T) {
	os.Setenv(internal.UploadConcurrencySetting, "invalid")
	_, err := internal.GetMaxConcurrency(internal.UploadConcurrencySetting)

	assert.Error(t, err)
	os.Unsetenv(internal.UploadConcurrencySetting)
}

func TestGetSentinelUserData(t *testing.T) {

	os.Setenv(internal.SentinelUserDataSetting, "1.0")

	data := internal.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, 1.0, data.(float64), "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Setenv(internal.SentinelUserDataSetting, "\"1\"")

	data = internal.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, "1", data.(string), "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Setenv(internal.SentinelUserDataSetting, `{"x":123,"y":["asdasd",123]}`)

	data = internal.GetSentinelUserData()
	t.Log(data)
	assert.NotNilf(t, data, "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Unsetenv(internal.UploadConcurrencySetting)
}
