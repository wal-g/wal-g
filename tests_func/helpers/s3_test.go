package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectKeyFromS3Prefix(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		bucket    string
		elements  []string
		expected  string
		expectErr bool
	}{
		{
			name:     "nested prefix",
			prefix:   "s3://dbaas/redis-backup/test_uuid/test_redis",
			bucket:   "dbaas",
			elements: []string{"basebackups_005", "20010203T040506", "redisdump.archive"},
			expected: "redis-backup/test_uuid/test_redis/basebackups_005/20010203T040506/redisdump.archive",
		},
		{
			name:     "normalizes slashes",
			prefix:   "s3://dbaas/root/",
			bucket:   "dbaas",
			elements: []string{"/basebackups_005/", "backup", "archive"},
			expected: "root/basebackups_005/backup/archive",
		},
		{name: "wrong scheme", prefix: "http://dbaas/root", bucket: "dbaas", expectErr: true},
		{name: "missing bucket", prefix: "s3:///root", bucket: "dbaas", expectErr: true},
		{name: "bucket mismatch", prefix: "s3://other/root", bucket: "dbaas", expectErr: true},
		{name: "query", prefix: "s3://dbaas/root?version=1", bucket: "dbaas", expectErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := ObjectKeyFromS3Prefix(test.prefix, test.bucket, test.elements...)
			if test.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestArchiveContainsTS(t *testing.T) {
	archive := Archive{
		StartTS: OpTimestamp{TS: 100, Inc: 2},
		EndTS:   OpTimestamp{TS: 101, Inc: 3},
	}

	tests := []struct {
		name     string
		ts       OpTimestamp
		expected bool
	}{
		{name: "before archive", ts: OpTimestamp{TS: 100, Inc: 1}},
		{name: "start belongs to previous archive", ts: OpTimestamp{TS: 100, Inc: 2}},
		{name: "inside archive", ts: OpTimestamp{TS: 100, Inc: 3}, expected: true},
		{name: "end is included", ts: OpTimestamp{TS: 101, Inc: 3}, expected: true},
		{name: "after archive", ts: OpTimestamp{TS: 101, Inc: 4}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, archiveContainsTS(archive, test.ts))
		})
	}
}
