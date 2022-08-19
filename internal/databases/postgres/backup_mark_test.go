package postgres_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type backupInfo map[string]struct {
	meta     postgres.ExtendedMetadataDto
	sentinel postgres.BackupSentinelDto
}

func TestGetBackupMetadataToUpload_markSeveralBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000006_D_000000010000000000000004"
	expectBackupsToMarkLen := 3
	expectBackupsToMark := map[int]string{
		0: "base_000000010000000000000002",
		1: "base_000000010000000000000004_D_000000010000000000000002",
		2: "base_000000010000000000000006_D_000000010000000000000004",
	}

	testGetBackupMetadataToUpload(backups, true, false, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func TestGetBackupMetadataToUpload_markOneBackup(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000006_D_000000010000000000000004"
	expectBackupsToMarkLen := 1
	expectBackupsToMark := map[int]string{
		0: "base_000000010000000000000006_D_000000010000000000000004",
	}
	testGetBackupMetadataToUpload(backups, true, false, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func TestGetBackupMetadataToUpload_unmarkOneBackupWithIncrementBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectBackupsToMarkLen := 1
	expectBackupsToMark := map[int]string{
		0: "base_000000010000000000000002",
	}

	testGetBackupMetadataToUpload(backups, false, false, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func TestGetBackupMetadataToUpload_unmarkOneBackupWithoutIncrementBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000004_D_000000010000000000000002"
	expectBackupsToMarkLen := 1
	expectBackupsToMark := map[int]string{
		0: "base_000000010000000000000004_D_000000010000000000000002",
	}

	testGetBackupMetadataToUpload(backups, false, false, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func TestGetBackupMetadataToUpload_tryToMarkAlreadyMarkedBackup(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectBackupsToMarkLen := 0
	expectBackupsToMark := map[int]string{}

	testGetBackupMetadataToUpload(backups, true, false, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func TestGetBackupMetadataToUpload_tryToUnmarkAlreadyUnmarkedBackup(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: false,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectBackupsToMarkLen := 0
	expectBackupsToMark := map[int]string{}

	testGetBackupMetadataToUpload(backups, false, false, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func TestGetBackupMetadataToUpload_tryToUnmarkBackupWithMarkedIncrementBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: postgres.ExtendedMetadataDto{
				IsPermanent: true,
			},
			sentinel: postgres.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i postgres.LSN) *postgres.LSN { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectBackupsToMarkLen := 0
	expectBackupsToMark := map[int]string{}
	testGetBackupMetadataToUpload(backups, false, true, toMark, expectBackupsToMarkLen, expectBackupsToMark, t)
}

func testGetBackupMetadataToUpload(
	backups backupInfo,
	toPermanent,
	isErrorExpect bool,
	toMark string,
	expectBackupsToMarkLen int,
	expectBackupsToMark map[int]string,
	t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
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
	markHandler := internal.NewBackupMarkHandler(postgres.NewGenericMetaInteractor(), folder)
	backupsToMark, err := markHandler.GetBackupsToMark(toMark, toPermanent)

	if !isErrorExpect {
		assert.NoError(t, err)
		assert.Equal(t, expectBackupsToMarkLen, len(backupsToMark))
		for idx, name := range expectBackupsToMark {
			assert.Equal(t, backupsToMark[idx], name)
		}
	} else {
		assert.Error(t, err)
	}
}
