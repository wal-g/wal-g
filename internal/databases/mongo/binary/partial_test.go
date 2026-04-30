package binary

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"reflect"
	"testing"
)

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
