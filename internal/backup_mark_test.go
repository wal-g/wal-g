package internal_test

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
	"testing"
)

func TestGetImpermanentBackupMetadataBefore(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backups := map[string]struct {
		meta     internal.ExtendedMetadataDto
		sentinel internal.BackupSentinelDto
	}{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: internal.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}

	for backupName, backupData := range backups {
		sentinelBytes, err := json.Marshal(backupData.sentinel)
		assert.NoError(t, err)
		err = baseBackupFolder.PutObject(backupName+utility.SentinelSuffix, bytes.NewReader(sentinelBytes))
		assert.NoError(t, err)

		metaBytes, err := json.Marshal(backupData.meta)
		assert.NoError(t, err)
		err = baseBackupFolder.PutObject(backupName+"/"+utility.MetadataFileName, bytes.NewReader(metaBytes))
		assert.NoError(t, err)
	}

	uploadObjects, err := internal.GetImpermanentBackupMetadataBefore(baseBackupFolder, "base_000000010000000000000006_D_000000010000000000000004")
	assert.NoError(t, err)
	assert.Equal(t, len(uploadObjects), 2)
	assert.Equal(t, uploadObjects[0].Path, "base_000000010000000000000002"+"/"+utility.MetadataFileName)
	assert.Equal(t, uploadObjects[1].Path, "base_000000010000000000000006_D_000000010000000000000004"+"/"+utility.MetadataFileName)
}
