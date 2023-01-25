package binary

import (
	"testing"

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
