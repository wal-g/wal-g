package binary

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMakeBsonRsMembers(t *testing.T) {
	assert.Equal(t, bson.A{}, makeBsonRsMembers(RsConfig{}))
	assert.Equal(t, bson.A{bson.M{"_id": 0, "host": "localhost:1234"}}, makeBsonRsMembers(RsConfig{
		RsMembers:   []string{"localhost:1234"},
		RsMemberIDs: []int{0},
	}))
	assert.Equal(t,
		bson.A{
			bson.M{"_id": 0, "host": "localhost:1234"},
			bson.M{"_id": 1, "host": "localhost:5678"},
			bson.M{"_id": 2, "host": "remotehost:9876"},
		},
		makeBsonRsMembers(RsConfig{
			RsName:      "",
			RsMembers:   []string{"localhost:1234", "localhost:5678", "remotehost:9876"},
			RsMemberIDs: []int{0, 1, 2},
		}))
	assert.Equal(t,
		bson.A{
			bson.M{"_id": 4, "host": "localhost:1234"},
			bson.M{"_id": 5, "host": "localhost:5678"},
			bson.M{"_id": 0, "host": "remotehost:9876"},
		},
		makeBsonRsMembers(RsConfig{
			RsName:      "",
			RsMembers:   []string{"localhost:1234", "localhost:5678", "remotehost:9876"},
			RsMemberIDs: []int{4, 5, 0},
		}))
}

func TestNewFollowLatestConfig(t *testing.T) {
	setViperTestValue(t, conf.OplogReplayFollowPollInterval, "2s")
	setViperTestValue(t, conf.OplogReplayCatchUpObservationWindow, "3m")
	setViperTestValue(t, conf.OplogReplayCatchUpTimeout, "4m")
	setViperTestValue(t, conf.OplogReplayStallTimeout, "5m")
	setViperTestValue(t, conf.OplogReplayCatchUpMaxLag, "6s")

	cfg, err := newFollowLatestConfig()

	require.NoError(t, err)
	require.Equal(t, 2*time.Second, cfg.PollInterval)
	require.Equal(t, 3*time.Minute, cfg.ObservationWindow)
	require.Equal(t, 4*time.Minute, cfg.CatchUpTimeout)
	require.Equal(t, 5*time.Minute, cfg.StallTimeout)
	require.Equal(t, 6*time.Second, cfg.MaxLag)
}

func TestNewFollowLatestConfigRejectsNonPositiveInterval(t *testing.T) {
	setViperTestValue(t, conf.OplogReplayFollowPollInterval, "0s")

	_, err := newFollowLatestConfig()

	require.ErrorContains(t, err, conf.OplogReplayFollowPollInterval+" must be positive")
}

func setViperTestValue(t *testing.T, key string, value any) {
	t.Helper()
	previous := viper.Get(key)
	wasSet := viper.IsSet(key)
	viper.Set(key, value)
	t.Cleanup(func() {
		if wasSet {
			viper.Set(key, previous)
		} else {
			viper.Set(key, nil)
		}
	})
}

func TestFollowLatestRequiresLatestTarget(t *testing.T) {
	_, err := NewReplyOplogConfig(t.Context(), "100.1", "200.1", false, false, true, "")

	require.ErrorContains(t, err, "follow-latest requires until to be LATEST")
}
