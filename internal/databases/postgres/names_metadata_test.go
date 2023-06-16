package postgres_test

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func genDatabasesByNames() postgres.DatabasesByNames {
	databasesByNames := make(postgres.DatabasesByNames)
	databasesByNames["my_database"] = *postgres.NewDatabaseObjectsInfo(20000)

	databasesByNames["my_database"].Tables["public.my_table"] = 30000
	databasesByNames["my_database"].Tables["namespace.other_table"] = 31000

	return databasesByNames
}

func TestDatabasesByNames_ResolveOrdinaryCase(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("my_database/my_table")
	assert.Equal(t, uint32(20000), dbID)
	assert.Equal(t, uint32(30000), tableID)
	assert.NoError(t, err)
}

func TestDatabasesByNames_ResolveNamespace(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("my_database/namespace.other_table")
	assert.Equal(t, uint32(20000), dbID)
	assert.Equal(t, uint32(31000), tableID)
	assert.NoError(t, err)
}

func TestDatabasesByNames_ResolveNoNamespaceSpecified(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("my_database/other_table")
	assert.Equal(t, uint32(0), dbID)
	assert.Equal(t, uint32(0), tableID)
	assert.Error(t, err)
}

func TestDatabasesByNames_ResolveOnlyDatabase(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("my_database")
	assert.Equal(t, uint32(20000), dbID)
	assert.Equal(t, uint32(0), tableID)
	assert.NoError(t, err)
}

func TestDatabasesByNames_ResolveNoSuchDatabase(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("no_exist_database/my_table")
	assert.Equal(t, uint32(0), dbID)
	assert.Equal(t, uint32(0), tableID)
	assert.Error(t, err)
}

func TestDatabasesByNames_ResolveTooManySlashes(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("my_folder/my_database/my_table")
	assert.Equal(t, uint32(0), dbID)
	assert.Equal(t, uint32(0), tableID)
	assert.Error(t, err)
}

func TestDatabasesByNames_ResolveDots(t *testing.T) {
	meta := genDatabasesByNames()

	dbID, tableID, err := meta.Resolve("my_database/public.my_table.my_column")
	assert.Equal(t, uint32(0), dbID)
	assert.Equal(t, uint32(0), tableID)
	assert.Error(t, err)
}
