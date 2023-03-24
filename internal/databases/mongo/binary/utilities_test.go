package binary

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnsureCompatibilityToRestoreMongodVersions(t *testing.T) {
	assert.Equal(t, nil, EnsureCompatibilityToRestoreMongodVersions("4.2", "4.4"))
	assert.Equal(t, nil, EnsureCompatibilityToRestoreMongodVersions("4.2", "4.2"))
	assert.Equal(t, nil, EnsureCompatibilityToRestoreMongodVersions("4.4", "4.4"))
	assert.Equal(t, nil, EnsureCompatibilityToRestoreMongodVersions("4.4", "5.0"))
	assert.Equal(t, nil, EnsureCompatibilityToRestoreMongodVersions("4.4.17-mdb-cdff3f4e", "5.0"))

	assert.Error(t, EnsureCompatibilityToRestoreMongodVersions("5.0", "4.4"))
	assert.Error(t, EnsureCompatibilityToRestoreMongodVersions("4.4", "4.2"))
}

func TestComputeMongoStartTimeout(t *testing.T) {
	var tb int64 = 1 << (10 * 4)
	var gb int64 = 1 << (10 * 3)

	assert.Equal(t, 200*time.Minute, ComputeMongoStartTimeout(5*tb))
	assert.Equal(t, 40*time.Minute, ComputeMongoStartTimeout(tb))
	assert.Equal(t, 20*time.Minute, ComputeMongoStartTimeout(tb/2))
	assert.Equal(t, 10*time.Minute, ComputeMongoStartTimeout(256*gb))
	assert.Equal(t, 10*time.Minute, ComputeMongoStartTimeout(20*gb))
	assert.Equal(t, 10*time.Minute, ComputeMongoStartTimeout(gb))

}
