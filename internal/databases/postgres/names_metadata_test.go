package postgres_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func genDatabasesByNames() postgres.DatabasesByNames {
	databasesByNames := make(postgres.DatabasesByNames)
	databasesByNames["my_database"] = *postgres.NewDatabaseObjectsInfo(20000)
	databasesByNames["db1"] = *postgres.NewDatabaseObjectsInfo(20001)
	databasesByNames["db2"] = *postgres.NewDatabaseObjectsInfo(20002)

	databasesByNames["my_database"].Tables["public.my_table"] = postgres.TableInfo{Oid: 30000, Relfilenode: 30000}
	databasesByNames["my_database"].Tables["namespace.other_table"] = postgres.TableInfo{Oid: 31000, Relfilenode: 31000}

	databasesByNames["db1"].Tables["public.table1"] = postgres.TableInfo{Oid: 40000, Relfilenode: 40000}
	databasesByNames["db1"].Tables["public.table2"] = postgres.TableInfo{Oid: 40001, Relfilenode: 40001}
	databasesByNames["db1"].Tables["public.tab1"] = postgres.TableInfo{Oid: 40002, Relfilenode: 40002}

	databasesByNames["db2"].Tables["my1.table1"] = postgres.TableInfo{Oid: 40100, Relfilenode: 40100}
	databasesByNames["db2"].Tables["my2.table2"] = postgres.TableInfo{Oid: 40101, Relfilenode: 40101}
	databasesByNames["db2"].Tables["nomy.tab3"] = postgres.TableInfo{Oid: 40102, Relfilenode: 40102}
	databasesByNames["db2"].Tables["nomy.tab4"] = postgres.TableInfo{Oid: 40103, Relfilenode: 40103}

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

	assert.Equal(t, 3, len(db1))
	sort.Slice(db1, func(i, j int) bool { return db1[i] < db1[j] })
	assert.Equal(t, uint32(40000), db1[0])
	assert.Equal(t, uint32(40001), db1[1])
	assert.Equal(t, uint32(40002), db1[2])
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

	assert.Equal(t, 3, len(db1))
	sort.Slice(db1, func(i, j int) bool { return db1[i] < db1[j] })
	assert.Equal(t, uint32(40000), db1[0])
	assert.Equal(t, uint32(40001), db1[1])
	assert.Equal(t, uint32(40002), db1[2])
	assert.Equal(t, 4, len(db2))
	sort.Slice(db2, func(i, j int) bool { return db2[i] < db2[j] })
	assert.Equal(t, uint32(40100), db2[0])
	assert.Equal(t, uint32(40101), db2[1])
	assert.Equal(t, uint32(40102), db2[2])
	assert.Equal(t, uint32(40103), db2[3])
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

	sort.Slice(db1, func(i, j int) bool { return db1[i] < db1[j] })
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

	sort.Slice(db1, func(i, j int) bool { return db1[i] < db1[j] })
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

	sort.Slice(db2, func(i, j int) bool { return db2[i] < db2[j] })
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

	sort.Slice(db2, func(i, j int) bool { return db2[i] < db2[j] })
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

func TestFetchDtoWithFilesMetadata(t *testing.T) {
	var folder = memory.NewFolder("in_memory/", memory.NewKVS())

	// Depricated struct
	type DatabaseObjectsInfoOld struct {
		Oid    uint32            `json:"oid"`
		Tables map[string]uint32 `json:"tables,omitempty"`
	}

	oldObjects := DatabaseObjectsInfoOld{Oid: 12, Tables: map[string]uint32{"t1": 1, "t2": 2}}
	newObjects := postgres.DatabaseObjectsInfo{Oid: 12, Tables: map[string]postgres.TableInfo{"t1": postgres.TableInfo{Oid: 1, Relfilenode: 2}, "t2": postgres.TableInfo{Oid: 3, Relfilenode: 4}}}

	bytesOld, _ := json.Marshal(oldObjects)
	bytesNew, _ := json.Marshal(newObjects)

	folder.PutObject("files_metadata_old.json", bytes.NewReader(bytesOld))
	folder.PutObject("files_metadata_new.json", bytes.NewReader(bytesNew))
	ansV1 := postgres.DatabaseObjectsInfo{}
	err := internal.FetchDto(folder, &ansV1, "files_metadata_old.json")
	assert.NoError(t, err)
	assert.Equal(t, uint32(12), ansV1.Oid)
	assert.Equal(t, uint32(0), ansV1.Tables["t1"].Oid)
	assert.Equal(t, uint32(0), ansV1.Tables["t1"].Relfilenode)
	assert.Equal(t, uint32(0), ansV1.Tables["t2"].Oid)
	assert.Equal(t, uint32(0), ansV1.Tables["t2"].Relfilenode)

	ansV2 := postgres.DatabaseObjectsInfo{}
	err = internal.FetchDto(folder, &ansV2, "files_metadata_new.json")
	assert.NoError(t, err)
	assert.Equal(t, uint32(12), ansV2.Oid)
	assert.Equal(t, uint32(1), ansV2.Tables["t1"].Oid)
	assert.Equal(t, uint32(2), ansV2.Tables["t1"].Relfilenode)
	assert.Equal(t, uint32(3), ansV2.Tables["t2"].Oid)
	assert.Equal(t, uint32(4), ansV2.Tables["t2"].Relfilenode)
}
