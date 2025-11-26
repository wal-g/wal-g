package partial

import (
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
				"testdb":       {"testcol": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Whitelist with only DB (no collection)",
			whitelist: []string{"testdb"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb":       {},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Multiple whitelist entries",
			whitelist: []string{"db1.col1", "db1.col2", "db2.col1"},
			blacklist: []string{},
			expectedWhitelist: map[string]map[string]struct{}{
				"db1":          {"col1": struct{}{}, "col2": struct{}{}},
				"db2":          {"col1": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Blacklist removes specific collection from whitelist",
			whitelist: []string{"testdb.col1", "testdb.col2"},
			blacklist: []string{"testdb.col1"},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb":       {"col2": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"mydb":         {"mycol": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Blacklist removes system database",
			whitelist: []string{"mydb.mycol"},
			blacklist: []string{"admin"},
			expectedWhitelist: map[string]map[string]struct{}{
				"mydb":         {"mycol": struct{}{}},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"db1":          {"col2": struct{}{}},
				"db3":          {},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"testdb":       {"col1": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{},
		},
		{
			name:      "Duplicate entries in blacklist",
			whitelist: []string{"testdb.col1", "testdb.col2"},
			blacklist: []string{"testdb.col1", "testdb.col1"},
			expectedWhitelist: map[string]map[string]struct{}{
				"testdb":       {"col2": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"testdb":       {"col1": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"db1":          {"col1": struct{}{}},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
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
				"db1":          {"col1": struct{}{}},
				"db2":          {},
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"db3": {"col1": struct{}{}},
				"db4": {},
			},
		},
		{
			name:      "All system databases removed by blacklist",
			whitelist: []string{"mydb.col1"},
			blacklist: []string{"admin", "local", "config", "mdb_internal"},
			expectedWhitelist: map[string]map[string]struct{}{
				"mydb": {"col1": struct{}{}},
			},
			expectedBlacklist: map[string]map[string]struct{}{
				"admin":        {},
				"local":        {},
				"config":       {},
				"mdb_internal": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWhitelist, gotBlacklist := GetFilters(tt.whitelist, tt.blacklist)

			if !reflect.DeepEqual(gotWhitelist, tt.expectedWhitelist) {
				t.Errorf("Whitelist mismatch\nGot:      %v\nExpected: %v", gotWhitelist, tt.expectedWhitelist)
			}

			if !reflect.DeepEqual(gotBlacklist, tt.expectedBlacklist) {
				t.Errorf("Blacklist mismatch\nGot:      %v\nExpected: %v", gotBlacklist, tt.expectedBlacklist)
			}
		})
	}
}
