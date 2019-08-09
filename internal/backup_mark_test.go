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

func TestGetBackupMetadataToUpload(t *testing.T) {
	type backupInfo map[string]struct {
		meta     internal.ExtendedMetadataDto
		sentinel internal.BackupSentinelDto
	}
	cases := []struct {
		backups                 backupInfo
		toMark                  string
		toPermanent             bool
		expectUploadObjectLen   int
		expectUploadObjectPaths map[int]string
	}{
		{
			backups: backupInfo{
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
			},
			toMark:                "base_000000010000000000000006_D_000000010000000000000004",
			toPermanent:           true,
			expectUploadObjectLen: 2,
			expectUploadObjectPaths: map[int]string{
				0: "base_000000010000000000000002" + "/" + utility.MetadataFileName,
				1: "base_000000010000000000000006_D_000000010000000000000004" + "/" + utility.MetadataFileName,

			},
		},
		{
			backups: backupInfo{
				"base_000000010000000000000002": {
					meta: internal.ExtendedMetadataDto{
						IsPermanent: true,
					},
					sentinel: internal.BackupSentinelDto{
						IncrementFrom: nil,
					},
				},
				"base_000000010000000000000004_D_000000010000000000000002": {
					meta: internal.ExtendedMetadataDto{
						IsPermanent: false,
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
						IsPermanent: true,
					},
					sentinel: internal.BackupSentinelDto{
						IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
						IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
						IncrementFullName: func(s string) *string { return &s }(""),
						IncrementCount:    func(i int) *int { return &i }(1),
					},
				},
			},
			toMark:                "base_000000010000000000000006_D_000000010000000000000004",
			toPermanent:           false,
			expectUploadObjectLen: 2,
			expectUploadObjectPaths: map[int]string{
				0: "base_000000010000000000000002" + "/" + utility.MetadataFileName,
				1: "base_000000010000000000000006_D_000000010000000000000004" + "/" + utility.MetadataFileName,

			},
		},
	}

	for _, c := range cases {
		folder := testtools.MakeDefaultInMemoryStorageFolder()
		baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
		for backupName, backupData := range c.backups {
		sentinelBytes, err := json.Marshal(backupData.sentinel)
		assert.NoError(t, err)
		err = baseBackupFolder.PutObject(backupName+utility.SentinelSuffix, bytes.NewReader(sentinelBytes))
		assert.NoError(t, err)

			metaBytes, err := json.Marshal(backupData.meta)
			assert.NoError(t, err)
			err = baseBackupFolder.PutObject(backupName+"/"+utility.MetadataFileName, bytes.NewReader(metaBytes))
			assert.NoError(t, err)
		}
		uploadObjects, err := internal.GetMarkedBackupMetadataToUpload(baseBackupFolder, c.toMark, c.toPermanent, true)
		assert.NoError(t, err)
		assert.Equal(t, len(uploadObjects), c.expectUploadObjectLen)
		for idx, path := range c.expectUploadObjectPaths {
			assert.Equal(t, uploadObjects[idx].Path, path)
		}
	}

	}
