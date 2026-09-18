package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

func TestIsPermanent_BackupPaths(t *testing.T) {
	permanentBackupName := "base_000000010000000E00000055"
	deltaBackupName := "base_000000010000000E00000059_D_000000010000000E00000055"
	permanentBackups := map[postgres.PermanentObject]bool{
		{Name: permanentBackupName, StorageName: "default"}:        true,
		{Name: deltaBackupName, StorageName: "default"}:            true,
	}
	permanentWals := map[postgres.PermanentObject]bool{}

	testCases := []struct {
		name       string
		objectPath string
		expected   bool
	}{
		{
			name:       "absolute sentinel path",
			objectPath: utility.BaseBackupPath + permanentBackupName + utility.SentinelSuffix,
			expected:   true,
		},
		{
			name:       "relative sentinel path from base backup subfolder",
			objectPath: permanentBackupName + utility.SentinelSuffix,
			expected:   true,
		},
		{
			name:       "relative metadata path from base backup subfolder",
			objectPath: permanentBackupName + "/" + utility.MetadataFileName,
			expected:   true,
		},
		{
			name:       "relative tar partition path from base backup subfolder",
			objectPath: permanentBackupName + "/tar_partitions/part_001.tar.br",
			expected:   true,
		},
		{
			name:       "permanent delta backup sentinel",
			objectPath: deltaBackupName + utility.SentinelSuffix,
			expected:   true,
		},
		{
			name:       "non-permanent backup",
			objectPath: "base_000000010000000E0000005B/" + utility.MetadataFileName,
			expected:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := postgres.IsPermanent(tc.objectPath, "default", permanentBackups, permanentWals)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestIsPermanent_WalPaths(t *testing.T) {
	permanentWalName := "000000010000000E00000055"
	permanentWals := map[postgres.PermanentObject]bool{
		{Name: permanentWalName, StorageName: "default"}: true,
	}

	testCases := []struct {
		name       string
		objectPath string
		expected   bool
	}{
		{
			name:       "absolute wal path",
			objectPath: utility.WalPath + permanentWalName + ".lz4",
			expected:   true,
		},
		{
			name:       "relative wal path from wal subfolder",
			objectPath: permanentWalName + ".lz4",
			expected:   true,
		},
		{
			name:       "unknown wal",
			objectPath: "000000010000000E0000005B.lz4",
			expected:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := postgres.IsPermanent(tc.objectPath, "default", map[postgres.PermanentObject]bool{}, permanentWals)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
