package multistorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/multistorage/stats/cache"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestNameAndOrderStorages(t *testing.T) {
	t.Run("sort storages by keys", func(t *testing.T) {
		primary := &testStorage{hash: "primary_hash"}
		failovers := map[string]storage.HashableStorage{
			"failover_1": &testStorage{hash: "failover_1_hash"},
			"failover_2": &testStorage{hash: "failover_2_hash"},
			"failover_3": &testStorage{hash: "failover_3_hash"},
		}
		namedStorages := NameAndOrderStorages(primary, failovers)
		want := NamedStorages{
			{
				Key: cache.Key{
					Name: "default",
					Hash: "primary_hash",
				},
				Name:            "default",
				HashableStorage: primary,
			},
			{
				Key: cache.Key{
					Name: "failover_1",
					Hash: "failover_1_hash",
				},
				Name:            "failover_1",
				HashableStorage: failovers["failover_1"],
			},
			{
				Key: cache.Key{
					Name: "failover_2",
					Hash: "failover_2_hash",
				},
				Name:            "failover_2",
				HashableStorage: failovers["failover_2"],
			},
			{
				Key: cache.Key{
					Name: "failover_3",
					Hash: "failover_3_hash",
				},
				Name:            "failover_3",
				HashableStorage: failovers["failover_3"],
			},
		}
		assert.Equal(t, want, namedStorages)
	})

	t.Run("works with no failover storages", func(t *testing.T) {
		primary := &testStorage{hash: "primary_hash"}
		namedStorages := NameAndOrderStorages(primary, nil)
		want := NamedStorages{
			{
				Key: cache.Key{
					Name: "default",
					Hash: "primary_hash",
				},
				Name:            "default",
				HashableStorage: primary,
			},
		}
		assert.Equal(t, want, namedStorages)
	})
}

func TestNamedStorages_Names(t *testing.T) {
	storages := NamedStorages{
		{Name: "default"},
		{Name: "failover_1"},
		{Name: "failover_2"},
		{Name: "failover_3"},
	}
	names := storages.Names()
	want := []string{"default", "failover_1", "failover_2", "failover_3"}
	assert.Equal(t, want, names)
}

func TestNamedStorages_Keys(t *testing.T) {
	storages := NamedStorages{
		{
			Key: cache.Key{
				Name: "default",
				Hash: "primary_hash",
			},
			Name: "default",
		},
		{
			Key: cache.Key{
				Name: "failover_1",
				Hash: "failover_1_hash",
			},
			Name: "failover_1",
		},
		{
			Key: cache.Key{
				Name: "failover_2",
				Hash: "failover_2_hash",
			},
			Name: "failover_2",
		},
		{
			Key: cache.Key{
				Name: "failover_3",
				Hash: "failover_3_hash",
			},
			Name: "failover_3",
		},
	}
	keys := storages.Keys()
	want := map[string]cache.Key{
		"default":    storages[0].Key,
		"failover_1": storages[1].Key,
		"failover_2": storages[2].Key,
		"failover_3": storages[3].Key,
	}
	assert.Equal(t, want, keys)
}

func TestNamedStorages_RootFolders(t *testing.T) {
	storages := NamedStorages{
		{
			Name: "default",
			HashableStorage: &testStorage{
				hash:       "default_hash",
				rootFolder: memory.NewFolder("default/", memory.NewKVS()),
			},
		},
		{
			Name: "failover_1",
			HashableStorage: &testStorage{
				hash:       "failover_1_hash",
				rootFolder: memory.NewFolder("failover_1/", memory.NewKVS()),
			},
		},
		{
			Name: "failover_2",
			HashableStorage: &testStorage{
				hash:       "failover_2_hash",
				rootFolder: memory.NewFolder("failover_2/", memory.NewKVS()),
			},
		},
		{
			Name: "failover_3",
			HashableStorage: &testStorage{
				hash:       "failover_3_hash",
				rootFolder: memory.NewFolder("failover_3/", memory.NewKVS()),
			},
		},
	}
	rootFolders := storages.RootFolders()
	want := map[string]storage.Folder{
		"default":    storages[0].RootFolder(),
		"failover_1": storages[1].RootFolder(),
		"failover_2": storages[2].RootFolder(),
		"failover_3": storages[3].RootFolder(),
	}
	assert.Equal(t, want, rootFolders)
}

var _ storage.HashableStorage = &testStorage{}

type testStorage struct {
	storage.Storage
	hash       string
	rootFolder storage.Folder
}

func (ts *testStorage) ConfigHash() string {
	return ts.hash
}

func (ts *testStorage) RootFolder() storage.Folder {
	return ts.rootFolder
}
