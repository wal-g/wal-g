package binary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestParseOplogReplayApplyBatchSize(t *testing.T) {
	for _, value := range []string{"0", "-1", "abc", "1.5"} {
		_, err := parseOplogReplayApplyBatchSize(value)
		require.ErrorContains(t, err, "OPLOG_REPLAY_APPLY_BATCH_SIZE must be a positive integer")
	}
	for _, tc := range []struct {
		value string
		size  int
	}{{"1", 1}, {"50", 50}, {"1000", 1000}} {
		size, err := parseOplogReplayApplyBatchSize(tc.value)
		require.NoError(t, err)
		assert.Equal(t, tc.size, size)
	}
}
