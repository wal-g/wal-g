package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func genDatabasesByNames() postgres.DatabasesByNames {
	databasesByNames := make(postgres.DatabasesByNames)
	databasesByNames["my_database"] = *postgres.NewDatabaseObjectsInfo(20000)
	databasesByNames["db1"] = *postgres.NewDatabaseObjectsInfo(20001)
	databasesByNames["db2"] = *postgres.NewDatabaseObjectsInfo(20002)

	databasesByNames["my_database"].Tables["public.my_table"] = 30000
	databasesByNames["my_database"].Tables["namespace.other_table"] = 31000

	databasesByNames["db1"].Tables["public.table1"] = 40000
	databasesByNames["db1"].Tables["public.table2"] = 40001
	databasesByNames["db1"].Tables["public.tab1"] = 40002

	databasesByNames["db2"].Tables["my1.table1"] = 40100
	databasesByNames["db2"].Tables["my2.table2"] = 40101
	databasesByNames["db2"].Tables["nomy.tab3"] = 40102
	databasesByNames["db2"].Tables["nomy.tab4"] = 40103

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

func TestResolveRegexp_RestoreAllForDatabase(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db1")
	db1, ok1 := dict[uint32(20001)]
	_, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.True(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 1, len(db1))
	assert.Equal(t, uint32(0), db1[0])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreSomeDatabase(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db*")
	db1, ok1 := dict[uint32(20001)]
	db2, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 1, len(db1))
	assert.Equal(t, uint32(0), db1[0])
	assert.Equal(t, 1, len(db2))
	assert.Equal(t, uint32(0), db2[0])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreAllForDatabaseRegexp(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db1/*")
	db1, ok1 := dict[uint32(20001)]
	_, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.True(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 3, len(db1))
	assert.Equal(t, uint32(40000), db1[0])
	assert.Equal(t, uint32(40001), db1[1])
	assert.Equal(t, uint32(40002), db1[2])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreSomeTablesInDatabase(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db1/table*")
	db1, ok1 := dict[uint32(20001)]
	_, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.True(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 2, len(db1))
	assert.Equal(t, uint32(40000), db1[0])
	assert.Equal(t, uint32(40001), db1[1])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreTableInDatabase(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db1/table1")
	db1, ok1 := dict[uint32(20001)]
	_, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.True(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 1, len(db1))
	assert.Equal(t, uint32(40000), db1[0])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreSomeNamespaces(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db2/my*/*")
	_, ok1 := dict[uint32(20001)]
	db2, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.False(t, ok1)
	assert.True(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 2, len(db2))
	assert.Equal(t, uint32(40100), db2[0])
	assert.Equal(t, uint32(40101), db2[1])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreAllInNamespace(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db2/nomy/*")
	_, ok1 := dict[uint32(20001)]
	db2, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.False(t, ok1)
	assert.True(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 2, len(db2))
	assert.Equal(t, uint32(40102), db2[0])
	assert.Equal(t, uint32(40103), db2[1])
	assert.NoError(t, err)
}

func TestResolveRegexp_RestoreTableInNamespace(t *testing.T) {
	meta := genDatabasesByNames()

	dict, err := meta.ResolveRegexp("db2/nomy/tab3")
	_, ok1 := dict[uint32(20001)]
	db2, ok2 := dict[uint32(20002)]
	_, ok3 := dict[uint32(20000)]
	assert.False(t, ok1)
	assert.True(t, ok2)
	assert.False(t, ok3)

	assert.Equal(t, 1, len(db2))
	assert.Equal(t, uint32(40102), db2[0])
	assert.NoError(t, err)
}
