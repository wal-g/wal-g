package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestReadDeprecatedFieldsRequiresSentinel(t *testing.T) {
	metadata := &FilesMetadataDto{
		Files: internal.BackupFileList{"current": {IsSkipped: true}},
	}
	backup := Backup{FilesMetadataDto: metadata}
	before := backup
	lsn := LSN(42)
	fields := DeprecatedSentinelFields{
		FilesMetadataDto: FilesMetadataDto{
			Files: internal.BackupFileList{"legacy": {IsIncremented: true}},
		},
		DeltaFromLSN: &lsn,
	}

	err := backup.readDeprecatedFields(fields)

	require.ErrorContains(t, err, "backup sentinel is not fetched")
	assert.Equal(t, before, backup, "an invalid call must not partially migrate metadata")
	assert.Same(t, metadata, backup.FilesMetadataDto)
	assert.Equal(t, internal.BackupFileList{"current": {IsSkipped: true}}, metadata.Files)
}

func TestReadDeprecatedFieldsPreservesCurrentMetadata(t *testing.T) {
	for _, hasMetadata := range []bool{false, true} {
		name := "without cached files metadata"
		if hasMetadata {
			name = "with cached files metadata"
		}
		t.Run(name, func(t *testing.T) {
			lsn := LSN(42)
			sentinel := &BackupSentinelDto{IncrementFromLSN: &lsn, PgVersion: 150000}
			backup := Backup{SentinelDto: sentinel}
			if hasMetadata {
				backup.FilesMetadataDto = &FilesMetadataDto{
					Files: internal.BackupFileList{"current": {IsSkipped: true}},
				}
			}
			metadata := backup.FilesMetadataDto
			wantLSN := LSN(42)
			want := BackupSentinelDto{IncrementFromLSN: &wantLSN, PgVersion: 150000}

			err := backup.readDeprecatedFields(DeprecatedSentinelFields{})

			require.NoError(t, err)
			assert.Same(t, sentinel, backup.SentinelDto)
			assert.Equal(t, want, *backup.SentinelDto)
			if hasMetadata {
				assert.Same(t, metadata, backup.FilesMetadataDto)
				assert.Equal(t, internal.BackupFileList{"current": {IsSkipped: true}}, backup.FilesMetadataDto.Files)
			} else {
				assert.Nil(t, backup.FilesMetadataDto)
			}
		})
	}
}

func TestReadDeprecatedFieldsRestoresFilesMetadata(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files internal.BackupFileList
	}{
		{name: "populated files", files: internal.BackupFileList{"base/1/2": {IsIncremented: true}}},
		{name: "empty non-nil files", files: internal.BackupFileList{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lsn := LSN(42)
			backup := Backup{
				SentinelDto: &BackupSentinelDto{IncrementFromLSN: &lsn, PgVersion: 150000},
				FilesMetadataDto: &FilesMetadataDto{
					Files: internal.BackupFileList{"current": {IsSkipped: true}},
				},
			}
			wantLSN := LSN(42)
			wantSentinel := BackupSentinelDto{IncrementFromLSN: &wantLSN, PgVersion: 150000}
			wantMetadata := FilesMetadataDto{
				Files:            tc.files,
				TarFileSets:      map[string][]string{"part_001.tar": {"base/1/2"}},
				DatabasesByNames: DatabasesByNames{"app": {Oid: 1}},
			}
			fields := DeprecatedSentinelFields{FilesMetadataDto: wantMetadata}

			err := backup.readDeprecatedFields(fields)

			require.NoError(t, err)
			require.NotNil(t, backup.FilesMetadataDto)
			assert.Equal(t, wantMetadata, *backup.FilesMetadataDto)
			assert.NotNil(t, backup.FilesMetadataDto.Files, "an empty legacy file list must not be treated as absent")
			assert.Equal(t, wantSentinel, *backup.SentinelDto, "file migration must preserve sentinel fields")
		})
	}
}

func TestReadDeprecatedFieldsRestoresLSN(t *testing.T) {
	for _, tc := range []struct {
		name string
		lsn  LSN
	}{
		{name: "nonzero LSN", lsn: 42},
		{name: "zero LSN", lsn: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, hasCurrentLSN := range []bool{false, true} {
				name := "sets missing LSN"
				if hasCurrentLSN {
					name = "replaces current LSN"
				}
				t.Run(name, func(t *testing.T) {
					metadata := &FilesMetadataDto{Files: internal.BackupFileList{"current": {IsSkipped: true}}}
					backup := Backup{
						SentinelDto:      &BackupSentinelDto{PgVersion: 150000},
						FilesMetadataDto: metadata,
					}
					if hasCurrentLSN {
						currentLSN := LSN(100)
						backup.SentinelDto.IncrementFromLSN = &currentLSN
					}
					fields := DeprecatedSentinelFields{DeltaFromLSN: &tc.lsn}
					wantLSN := tc.lsn
					want := BackupSentinelDto{IncrementFromLSN: &wantLSN, PgVersion: 150000}

					err := backup.readDeprecatedFields(fields)

					require.NoError(t, err)
					assert.Equal(t, want, *backup.SentinelDto)
					assert.Same(t, metadata, backup.FilesMetadataDto)
					assert.Equal(t, internal.BackupFileList{"current": {IsSkipped: true}}, backup.FilesMetadataDto.Files)
				})
			}
		})
	}
}

func TestReadDeprecatedFieldsRestoresFilesAndLSN(t *testing.T) {
	lsn := LSN(42)
	backup := Backup{SentinelDto: &BackupSentinelDto{PgVersion: 150000}}
	wantMetadata := FilesMetadataDto{
		Files: internal.BackupFileList{"base/1/2": {IsIncremented: true}},
	}
	fields := DeprecatedSentinelFields{
		FilesMetadataDto: wantMetadata,
		DeltaFromLSN:     &lsn,
	}

	err := backup.readDeprecatedFields(fields)

	require.NoError(t, err)
	require.NotNil(t, backup.FilesMetadataDto)
	assert.Equal(t, wantMetadata, *backup.FilesMetadataDto)
	wantLSN := LSN(42)
	assert.Equal(t, BackupSentinelDto{IncrementFromLSN: &wantLSN, PgVersion: 150000}, *backup.SentinelDto)
}
