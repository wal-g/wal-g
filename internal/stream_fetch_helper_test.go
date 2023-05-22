package internal

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

const (
	endTSEnvVar                 = "someTSEnvVar"
	operationLogsDstEnvVariable = "someOperationLogsDstEnv"
)

func TestParseTs_shouldParseRFC3339(t *testing.T) {
	currentTime := time.Now().UTC().Truncate(time.Second)
	viper.Set(endTSEnvVar, currentTime.Format(time.RFC3339))
	parsedTime, err := ParseTS(endTSEnvVar)
	viper.Set(endTSEnvVar, nil)
	assert.NoError(t, err)
	assert.Equal(t, &currentTime, parsedTime)
	resetToDefaults()
}

func TestParseTs_shouldFailOnWrongFormat(t *testing.T) {
	currentTime := time.Now().UTC().Truncate(time.Second)
	viper.Set(endTSEnvVar, currentTime.Format(time.RFC822))
	parsedTime, err := ParseTS(endTSEnvVar)
	viper.Set(endTSEnvVar, nil)
	assert.Error(t, err)
	assert.Nil(t, parsedTime)
	resetToDefaults()
}

func TestParseTs_shouldFailOnBadTimeString(t *testing.T) {
	viper.Set(endTSEnvVar, "some_total_gibberish_no_time_string_at_all")
	parsedTime, err := ParseTS(endTSEnvVar)
	viper.Set(endTSEnvVar, nil)
	assert.Error(t, err)
	assert.Nil(t, parsedTime)
	resetToDefaults()
}

func TestParseTs_shouldReturnNilOnNoTime(t *testing.T) {
	parsedTime, err := ParseTS(endTSEnvVar)
	assert.NoError(t, err)
	assert.Nil(t, parsedTime)
	resetToDefaults()
}

func TestGetLogsDstSettings_simpleCase(t *testing.T) {
	directoryMock := "some_kek_dir"
	viper.Set(operationLogsDstEnvVariable, directoryMock)
	parsedDirectory, err := GetLogsDstSettings(operationLogsDstEnvVariable)
	assert.NoError(t, err)
	assert.Equal(t, directoryMock, parsedDirectory)
	resetToDefaults()
}

func TestGetLogsDstSettings_shouldReturnNilOnNoDirectory(t *testing.T) {
	parsedDirectory, err := GetLogsDstSettings(operationLogsDstEnvVariable)
	assert.Error(t, err)
	assert.Equal(t, "", parsedDirectory)
	resetToDefaults()
}

func resetToDefaults() {
	viper.Reset()
	ConfigureSettings(PG)
	InitConfig()
	Configure()
}
