package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func TestTryGetOidPair_DBFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/1234/5678_fsm.1")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1234, dbID)
	assert.Equal(t, 5678, tableID)
}

func TestTryGetOidPair_RandomFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/path/to/files/1/2")
	assert.Equal(t, false, isDB)
	assert.Equal(t, 0, dbID)
	assert.Equal(t, 0, tableID)
}
