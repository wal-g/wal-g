package binary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMakeBsonRsMembers(t *testing.T) {
	assert.Equal(t, bson.A{}, makeBsonRsMembers(RsConfig{}))
	assert.Equal(t, bson.A{bson.M{"_id": 0, "host": "localhost:1234"}}, makeBsonRsMembers(RsConfig{
		RsMembers: []string{"localhost:1234"},
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
			RsMemberIds: []int{},
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
			RsMemberIds: []int{4, 5, 0},
		}))
}
