package binary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMakeBsonRsMembers(t *testing.T) {
	assert.Equal(t, bson.A{}, makeBsonRsMembers(""))
	assert.Equal(t, bson.A{bson.M{"_id": 0, "host": "localhost:1234"}}, makeBsonRsMembers("localhost:1234"))
	assert.Equal(t,
		bson.A{
			bson.M{"_id": 0, "host": "localhost:1234"},
			bson.M{"_id": 1, "host": "localhost:5678"},
			bson.M{"_id": 2, "host": "remotehost:9876"},
		},
		makeBsonRsMembers("localhost:1234,localhost:5678,remotehost:9876"))
}
