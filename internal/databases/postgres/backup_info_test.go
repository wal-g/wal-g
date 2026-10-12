package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestBackupInfo_PrintableFields(t *testing.T) {
	tests := []struct {
		name     string
		backup   BackupInfo
		expected []printlist.TableField
	}{
		{
			name: "standard backup info",
			backup: BackupInfo{
				Name:    "backup_name_123",
				Storage: "s3",
			},
			expected: []printlist.TableField{
				{
					Name:       "name",
					PrettyName: "Name of backup",
					Value:      "backup_name_123",
				},
				{
					Name:       "storage",
					PrettyName: "Name of storage",
					Value:      "s3",
				},
			},
		},
		{
			name: "empty backup info",
			backup: BackupInfo{
				Name:    "",
				Storage: "",
			},
			expected: []printlist.TableField{
				{
					Name:       "name",
					PrettyName: "Name of backup",
					Value:      "",
				},
				{
					Name:       "storage",
					PrettyName: "Name of storage",
					Value:      "",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.backup.PrintableFields()
			assert.Equal(t, tt.expected, actual)
		})
	}
}
