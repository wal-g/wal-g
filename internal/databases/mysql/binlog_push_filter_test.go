package mysql

import (
	"slices"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestFilterBinlogsForUpload(t *testing.T) {
	binlogs := []string{
		binlog1,
		binlog2,
		binlog3,
		binlog4,
		binlog5,
		binlog6,
	}
	cases := []struct {
		name         string
		inputBinlogs []string
		localCache   string
		until        string
		gtidArchived string
		expected     []string
	}{
		{
			name:         "no files",
			inputBinlogs: []string{},
			localCache:   "",
			until:        "",
			gtidArchived: "",
			expected:     []string{},
		},
		{
			name:         "only active file",
			inputBinlogs: binlogs[5:],
			localCache:   "",
			until:        "",
			gtidArchived: "",
			expected:     []string{},
		},
		{
			name:         "no filters",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        "",
			gtidArchived: "",
			expected:     binlogs,
		},
		{
			name:         "skip cached prefix",
			inputBinlogs: binlogs,
			localCache:   binlogs[1],
			until:        "",
			gtidArchived: "",
			expected:     binlogs[2:],
		},
		{
			name:         "cache before first file",
			inputBinlogs: binlogs,
			localCache:   "mysql-bin.000000",
			until:        "",
			gtidArchived: "",
			expected:     binlogs,
		},
		{
			name:         "all completed files cached",
			inputBinlogs: binlogs,
			localCache:   binlogs[4],
			until:        "",
			gtidArchived: "",
			expected:     []string{},
		},
		{
			name:         "apply until filter",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        binlogs[2],
			gtidArchived: "",
			expected:     binlogs[:3],
		},
		{
			name:         "until at first file",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        binlogs[0],
			gtidArchived: "",
			expected:     []string{},
		},
		{
			name:         "until before first file",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        "mysql-bin.000000",
			gtidArchived: "",
			expected:     []string{},
		},
		{
			name:         "until beyond active file",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        "mysql-bin.000100",
			gtidArchived: "",
			expected:     binlogs,
		},
		{
			name:         "until before cache",
			inputBinlogs: binlogs,
			localCache:   binlogs[1],
			until:        binlogs[0],
			gtidArchived: "",
			expected:     []string{},
		},
		{
			name:         "some files are filtered out by GTID filter",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        "",
			gtidArchived: testDataBinlogServerUuid + ":1-2",
			expected:     binlogs[2:],
		},
		{
			name:         "all files are filtered out by GTID filter",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        "",
			gtidArchived: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5",
			expected:     binlogs,
		},
		{
			name:         "no files are filtered out by GTID filter",
			inputBinlogs: binlogs,
			localCache:   "",
			until:        "",
			gtidArchived: testDataBinlogServerUuid + ":1-5",
			expected:     []string{},
		},
		{
			name:         "all filters together",
			inputBinlogs: binlogs,
			localCache:   binlogs[0],
			until:        binlogs[3],
			gtidArchived: testDataBinlogServerUuid + ":1-2",
			expected:     binlogs[2:4],
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archived, err := mysql.ParseMysqlGTIDSet(tc.gtidArchived)
			require.NoError(t, err)
			filter := &mysqlGtidFilter{
				binlogsFolder: "testdata/binlogs",
				flavor:        mysql.MySQLFlavor,
				gtidArchived:  archived.(*mysql.MysqlGTIDSet),
			}
			original := slices.Clone(tc.inputBinlogs)
			got := filterBinlogsForUpload(tc.inputBinlogs, tc.localCache, tc.until, filter)
			require.Equal(t, tc.expected, got)
			require.Equal(t, original, tc.inputBinlogs, "filtering must not modify the binlog list")
		})
	}
}
