package internal

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

const (
	endTSEnvVar = "someTSEnvVar"
)

func TestParseTs_shouldParseRFC3339(t *testing.T) {
	currentTime := time.Now().UTC().Truncate(time.Second)
	viper.Set(endTSEnvVar, currentTime.Format(time.RFC3339))
	parsedTime, err := ParseTS(endTSEnvVar)
	viper.Set(endTSEnvVar, nil)
	assert.NoError(t, err)
	assert.Equal(t, &currentTime, parsedTime)
}

func TestParseTs_shouldFailOnWrongFormat(t *testing.T) {
	currentTime := time.Now().UTC().Truncate(time.Second)
	viper.Set(endTSEnvVar, currentTime.Format(time.RFC822))
	parsedTime, err := ParseTS(endTSEnvVar)
	viper.Set(endTSEnvVar, nil)
	assert.Error(t, err)
	assert.Nil(t, parsedTime)
}

func TestParseTs_shouldFailOnBadTimeString(t *testing.T) {
	viper.Set(endTSEnvVar, "some_total_gibberish_no_time_string_at_all")
	parsedTime, err := ParseTS(endTSEnvVar)
	viper.Set(endTSEnvVar, nil)
	assert.Error(t, err)
	assert.Nil(t, parsedTime)
}

func TestParseTs_shouldReturnNilOnNoTime(t *testing.T) {
	parsedTime, err := ParseTS(endTSEnvVar)
	assert.NoError(t, err)
	assert.Nil(t, parsedTime)
}
