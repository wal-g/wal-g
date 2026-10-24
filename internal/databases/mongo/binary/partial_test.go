package binary

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestEnrichWithTarPathsIncludesAllFileParts(t *testing.T) {
	collector := NewPartialMetadataCollector()
	collector.HandleNsInfo(makeNsInfo("testdb.selected", "statistics:table:collection-1", map[string]string{
		"_id_": "statistics:table:index-1",
	}))
	collector.HandleNsInfo(makeNsInfo("testdb.excluded", "statistics:table:collection-2", nil))

	require.NoError(t, collector.EnrichWithTarPaths(map[string][]string{
		"part_008.tar.lz4": {"/collection-1.wt"},
		"part_001.tar.lz4": {"/collection-1.wt", "/collection-1.wt"},
		"part_002.tar.lz4": {"/index-1.wt"},
		"part_003.tar.lz4": {"/index-1.wt"},
		"part_004.tar.lz4": {"/collection-2.wt"},
		"part_005.tar.lz4": {"/WiredTiger", "/WiredTiger.wt"},
		"part_006.tar.lz4": {"/collection-orphan.wt"},
		"part_007.tar.lz4": {"/collection-orphan.wt"},
	}))

	routes := collector.GetRoutes()
	selected := routes.Databases["testdb"]["selected"]
	assert.Equal(t, "part_001.tar.lz4", selected.TarPath)
	assert.Equal(t, []string{"part_001.tar.lz4", "part_008.tar.lz4"}, selected.TarPaths)
	assert.Equal(t, "part_002.tar.lz4", selected.IndexInfo["_id_"].TarPath)
	assert.Equal(t, []string{"part_002.tar.lz4", "part_003.tar.lz4"}, selected.IndexInfo["_id_"].TarPaths)
	assert.Nil(t, routes.Databases["testdb"]["excluded"].TarPaths)
	assert.Equal(t, map[string]string{
		"/WiredTiger":           "part_005.tar.lz4",
		"/WiredTiger.wt":        "part_005.tar.lz4",
		"/collection-orphan.wt": "part_006.tar.lz4",
	}, routes.Service)
	assert.Equal(t, map[string][]string{
		"/collection-orphan.wt": {"part_006.tar.lz4", "part_007.tar.lz4"},
	}, routes.ServiceTarPaths)

	encoded, err := json.Marshal(routes)
	require.NoError(t, err)
	var decoded models.BackupRoutesInfo
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	for _, filters := range []struct {
		name      string
		whitelist []string
		blacklist []string
	}{
		{name: "whitelist", whitelist: []string{"testdb.selected"}},
		{name: "blacklist", blacklist: []string{"testdb.excluded"}},
	} {
		t.Run(filters.name, func(t *testing.T) {
			paths, archives := GetTarFilesFilter(&decoded, filters.whitelist, filters.blacklist)
			assert.Equal(t, map[string]struct{}{
				"/collection-1.wt":      {},
				"/index-1.wt":           {},
				"/WiredTiger":           {},
				"/WiredTiger.wt":        {},
				"/collection-orphan.wt": {},
			}, paths)
			assert.Equal(t, map[string]struct{}{
				"part_001.tar.lz4": {},
				"part_002.tar.lz4": {},
				"part_003.tar.lz4": {},
				"part_005.tar.lz4": {},
				"part_006.tar.lz4": {},
				"part_007.tar.lz4": {},
				"part_008.tar.lz4": {},
			}, archives)
		})
	}
}

func TestEnrichWithTarPathsMissingFile(t *testing.T) {
	for _, missing := range []string{"/collection-1.wt", "/index-1.wt"} {
		t.Run(missing, func(t *testing.T) {
			collector := NewPartialMetadataCollector()
			collector.HandleNsInfo(makeNsInfo("testdb.selected", "statistics:table:collection-1", map[string]string{
				"_id_": "statistics:table:index-1",
			}))
			tarPaths := map[string][]string{
				"collection.tar.lz4": {"/collection-1.wt"},
				"index.tar.lz4":      {"/index-1.wt"},
			}
			if missing == "/collection-1.wt" {
				delete(tarPaths, "collection.tar.lz4")
			} else {
				delete(tarPaths, "index.tar.lz4")
			}
			require.EqualError(t, collector.EnrichWithTarPaths(tarPaths), "file "+missing+" not found in tar directory")
		})
	}
}

func TestGetTarFilesFilterLegacyMetadata(t *testing.T) {
	const metadata = `{
		"databases": {"testdb": {"selected": {
			"paths": {"db_path": "/collection-1.wt", "tar_path": "collection.tar.lz4"},
			"index_info": {"_id_": {"db_path": "/index-1.wt", "tar_path": "index.tar.lz4"}}
		}}},
		"service": {"/WiredTiger": "service.tar.lz4"}
	}`
	var routes models.BackupRoutesInfo
	require.NoError(t, json.Unmarshal([]byte(metadata), &routes))
	paths, archives := GetTarFilesFilter(&routes, []string{"testdb.selected"}, nil)
	assert.Equal(t, map[string]struct{}{
		"/collection-1.wt": {},
		"/index-1.wt":      {},
		"/WiredTiger":      {},
	}, paths)
	assert.Equal(t, map[string]struct{}{
		"collection.tar.lz4": {},
		"index.tar.lz4":      {},
		"service.tar.lz4":    {},
	}, archives)
	encoded, err := json.Marshal(&routes)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "tar_paths")
}

func TestGetFilters(t *testing.T) {
	tests := []struct {
		name              string
		whitelist         []string
		blacklist         []string
		expectedWhitelist map[string]map[string]struct{}
		expectedBlacklist map[string]map[string]struct{}
	}{
		{
			name:              "Empty whitelist and blacklist",
			whitelist:         []string{},
			blacklist:         []string{},
			expectedWhitelist: map[string]map[string]struct{}{},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:              "Nil whitelist and blacklist",
			whitelist:         nil,
			blacklist:         nil,
			expectedWhitelist: map[string]map[string]struct{}{},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Only whitelist with DB and collection",
			whitelist: []string{"testdb.testcol"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb": {"testcol": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Whitelist with only DB (no collection)",
			whitelist: []string{"testdb"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb": {},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Multiple whitelist entries",
			whitelist: []string{"db1.col1", "db1.col2", "db2.col1"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"db1":    {"col1": struct{}{}, "col2": struct{}{}},
				"db2":    {"col1": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Blacklist removes specific collection from whitelist",
			whitelist: []string{"testdb.col1", "testdb.col2"},
			blacklist: []string{"testdb.col1"},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb": {"col2": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"testdb": {"col1": struct{}{}},
			},
		},
		{
			name:      "Blacklist removes entire DB from whitelist",
			whitelist: []string{"testdb.col1", "testdb.col2"},
			blacklist: []string{"testdb"},
			expectedWhitelist: map[string]map[string]struct{}{
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"testdb": {},
			},
		},
		{
			name:      "Only blacklist without whitelist",
			whitelist: []string{},
			blacklist: []string{"testdb.col1"},
			expectedWhitelist: map[string]map[string]struct{}{
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"testdb": {"col1": struct{}{}},
			},
		},
		{
			name:      "Blacklist with only DB name",
			whitelist: []string{},
			blacklist: []string{"testdb"},
			expectedWhitelist: map[string]map[string]struct{}{
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"testdb": {},
			},
		},
		{
			name:      "System databases always in whitelist",
			whitelist: []string{"mydb.mycol"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"mydb":   {"mycol": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Blacklist removes system database",
			whitelist: []string{"mydb.mycol"},
			blacklist: []string{"admin"},
			expectedWhitelist: map[string]map[string]struct{}{
				"mydb":   {"mycol": struct{}{}},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"admin": {},
			},
		},
		{
			name:      "Complex scenario with multiple operations",
			whitelist: []string{"db1.col1", "db1.col2", "db2.col1", "db3"},
			blacklist: []string{"db1.col1", "db2"},
			expectedWhitelist: map[string]map[string]struct{}{
				"db1":    {"col2": struct{}{}},
				"db3":    {},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"db1": {"col1": struct{}{}},
				"db2": {},
			},
		},
		{
			name:      "Duplicate entries in whitelist",
			whitelist: []string{"testdb.col1", "testdb.col1", "testdb.col1"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb": {"col1": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Duplicate entries in blacklist",
			whitelist: []string{"testdb.col1", "testdb.col2"},
			blacklist: []string{"testdb.col1", "testdb.col1"},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb": {"col2": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"testdb": {"col1": struct{}{}},
			},
		},
		{
			name:      "Blacklist collection that doesn't exist in whitelist",
			whitelist: []string{"testdb.col1"},
			blacklist: []string{"testdb.col2"},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb": {"col1": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"testdb": {"col2": struct{}{}},
			},
		},
		{
			name:      "Blacklist DB that doesn't exist in whitelist",
			whitelist: []string{"db1.col1"},
			blacklist: []string{"db2"},
			expectedWhitelist: map[string]map[string]struct{}{
				"db1":    {"col1": struct{}{}},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"db2": {},
			},
		},
		{
			name:      "Mixed DB with and without collections",
			whitelist: []string{"db1.col1", "db2"},
			blacklist: []string{"db3.col1", "db4"},
			expectedWhitelist: map[string]map[string]struct{}{
				"db1":    {"col1": struct{}{}},
				"db2":    {},
				"admin":  {},
				"local":  {},
				"config": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"db3": {"col1": struct{}{}},
				"db4": {},
			},
		},
		{
			name:      "All system databases removed by blacklist",
			whitelist: []string{"mydb.col1"},
			blacklist: []string{"admin", "local", "config"},
			expectedWhitelist: map[string]map[string]struct{}{
				"mydb": {"col1": struct{}{}},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"admin":  {},
				"local":  {},
				"config": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWhitelist, gotBlacklist := getFilters(tt.whitelist, tt.blacklist)

			if !reflect.DeepEqual(gotWhitelist, tt.expectedWhitelist) {
				t.Errorf("Whitelist mismatch\nGot:      %v\nExpected: %v", gotWhitelist, tt.expectedWhitelist)
			}

			if !reflect.DeepEqual(gotBlacklist, tt.expectedBlacklist) {
				t.Errorf("Blacklist mismatch\nGot:      %v\nExpected: %v", gotBlacklist, tt.expectedBlacklist)
			}
		})
	}
}

func makeNsInfo(ns, collectionURI string, indexURIs map[string]string) *models.NsInfo {
	nsInfo := &models.NsInfo{}
	nsInfo.Ns = ns
	nsInfo.StorageStats.WiredTiger.URI = collectionURI
	nsInfo.StorageStats.IndexDetails = make(map[string]struct {
		URI string `bson:"uri"`
	})
	for name, uri := range indexURIs {
		nsInfo.StorageStats.IndexDetails[name] = struct {
			URI string `bson:"uri"`
		}{URI: uri}
	}
	return nsInfo
}

func TestHandleNsInfo(t *testing.T) {
	tests := []struct {
		name     string
		nsInfos  []*models.NsInfo
		expected map[string]models.DBInfo
	}{
		{
			name: "single collection without indexes",
			nsInfos: []*models.NsInfo{
				makeNsInfo("testdb.testcol", "statistics:table:collection-1-1234", nil),
			},
			expected: map[string]models.DBInfo{
				"testdb": {
					"testcol": models.CollectionInfo{
						Paths:     models.Paths{DBPath: "/collection-1-1234.wt"},
						IndexInfo: models.IndexInfo{},
					},
				},
			},
		},
		{
			name: "single collection with single index",
			nsInfos: []*models.NsInfo{
				makeNsInfo("testdb.testcol", "statistics:table:collection-1-1234", map[string]string{
					"id_": "statistics:table:index-1-1234",
				}),
			},
			expected: map[string]models.DBInfo{
				"testdb": {
					"testcol": models.CollectionInfo{
						Paths: models.Paths{DBPath: "/collection-1-1234.wt"},
						IndexInfo: models.IndexInfo{
							"id_": models.Paths{DBPath: "/index-1-1234.wt"},
						},
					},
				},
			},
		},
		{
			name: "single collection with multiple indexes",
			nsInfos: []*models.NsInfo{
				makeNsInfo("testdb.testcol", "statistics:table:collection-1-1234", map[string]string{
					"id_":    "statistics:table:index-1-1234",
					"name_1": "statistics:table:index-2-1234",
				}),
			},
			expected: map[string]models.DBInfo{
				"testdb": {
					"testcol": models.CollectionInfo{
						Paths: models.Paths{DBPath: "/collection-1-1234.wt"},
						IndexInfo: models.IndexInfo{
							"id_":    models.Paths{DBPath: "/index-1-1234.wt"},
							"name_1": models.Paths{DBPath: "/index-2-1234.wt"},
						},
					},
				},
			},
		},
		{
			name: "index with empty URI is skipped",
			nsInfos: []*models.NsInfo{
				makeNsInfo("testdb.testcol", "statistics:table:collection-1-1234", map[string]string{
					"id_":    "",
					"name_1": "statistics:table:index-2-1234",
				}),
			},
			expected: map[string]models.DBInfo{
				"testdb": {
					"testcol": models.CollectionInfo{
						Paths: models.Paths{DBPath: "/collection-1-1234.wt"},
						IndexInfo: models.IndexInfo{
							"name_1": models.Paths{DBPath: "/index-2-1234.wt"},
						},
					},
				},
			},
		},
		{
			name: "multiple collections in same db",
			nsInfos: []*models.NsInfo{
				makeNsInfo("testdb.col1", "statistics:table:collection-1-1234", nil),
				makeNsInfo("testdb.col2", "statistics:table:collection-2-1234", nil),
			},
			expected: map[string]models.DBInfo{
				"testdb": {
					"col1": models.CollectionInfo{
						Paths:     models.Paths{DBPath: "/collection-1-1234.wt"},
						IndexInfo: models.IndexInfo{},
					},
					"col2": models.CollectionInfo{
						Paths:     models.Paths{DBPath: "/collection-2-1234.wt"},
						IndexInfo: models.IndexInfo{},
					},
				},
			},
		},
		{
			name: "collections in different dbs",
			nsInfos: []*models.NsInfo{
				makeNsInfo("db1.col1", "statistics:table:collection-1-1234", nil),
				makeNsInfo("db2.col1", "statistics:table:collection-2-1234", nil),
			},
			expected: map[string]models.DBInfo{
				"db1": {
					"col1": models.CollectionInfo{
						Paths:     models.Paths{DBPath: "/collection-1-1234.wt"},
						IndexInfo: models.IndexInfo{},
					},
				},
				"db2": {
					"col1": models.CollectionInfo{
						Paths:     models.Paths{DBPath: "/collection-2-1234.wt"},
						IndexInfo: models.IndexInfo{},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pmc := NewPartialMetadataCollector()
			for _, nsInfo := range tt.nsInfos {
				pmc.HandleNsInfo(nsInfo)
			}
			assert.Equal(t, tt.expected, pmc.routes.Databases)
		})
	}
}
