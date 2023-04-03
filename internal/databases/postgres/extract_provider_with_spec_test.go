package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func TestTryGetOidPair_DefaultFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/1234/5678")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1234, dbID)
	assert.Equal(t, 5678, tableID)
}

func TestTryGetOidPair_VMFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/1234/5678_vm")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1234, dbID)
	assert.Equal(t, 5678, tableID)
}

func TestTryGetOidPair_PartFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/1234/5678.1")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1234, dbID)
	assert.Equal(t, 5678, tableID)
}

func TestTryGetOidPair_PartFSMFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/1234/5678_fsm.1")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1234, dbID)
	assert.Equal(t, 5678, tableID)
}

func TestTryGetOidPair_TablespaceFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/pg_tblspc/path/in/tablespace/1234/5678")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1234, dbID)
	assert.Equal(t, 5678, tableID)
}

func TestTryGetOidPair_SpecialFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/4/pg_filenode.map")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 4, dbID)
	assert.Equal(t, 0, tableID)
}

func TestTryGetOidPair_RandomFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/path/to/files/1/2")
	assert.Equal(t, false, isDB)
	assert.Equal(t, 0, dbID)
	assert.Equal(t, 0, tableID)
}

func TestTryGetOidPair_DirBetweenFile(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base/somedir/1/2")
	assert.Equal(t, true, isDB)
	assert.Equal(t, 1, dbID)
	assert.Equal(t, 2, tableID)
}

func TestTryGetOidPair_BaseRoot(t *testing.T) {
	p := postgres.ExtractProviderDBSpec{}
	isDB, dbID, tableID := p.TryGetOidPair("/base")
	assert.Equal(t, false, isDB)
	assert.Equal(t, 0, dbID)
	assert.Equal(t, 0, tableID)
}
