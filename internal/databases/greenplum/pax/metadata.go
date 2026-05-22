package pax

import (
	"time"
)

const FilesMetadataName = "pax_files_metadata.json"

// GetFilesMetadataPath returns the storage path of the PAX files metadata DTO
// inside the segment backup folder.
func GetFilesMetadataPath(backupName string) string {
	return backupName + "/" + FilesMetadataName
}

// BackupFileDesc describes a single PAX file that participates in a backup.
// One BackupFileDesc is produced per (data | toast | visimap) file referenced
// from `pg_ext_aux.pg_pax_blocks_*` at backup-start.
type BackupFileDesc struct {
	StoragePath     string    `json:"StoragePath"`
	IsSkipped       bool      `json:"IsSkipped,omitempty"`
	MTime           time.Time `json:"MTime"`
	RelNameMd5      string    `json:"RelNameMd5"`
	Kind            FileKind  `json:"Kind"`
	BlockID         int64     `json:"BlockID,omitempty"`
	FileMode        int64     `json:"FileMode"`
	InitialUploadTS time.Time `json:"InitialUploadTS,omitempty"`
}

// BackupFiles maps the file's PGDATA-relative path to its catalog/storage descriptor.
type BackupFiles map[string]BackupFileDesc

// FilesMetadataDTO is the shape persisted to `pax_files_metadata.json`.
type FilesMetadataDTO struct {
	Files BackupFiles
}

func NewFilesMetadataDTO() *FilesMetadataDTO {
	return &FilesMetadataDTO{Files: make(BackupFiles)}
}

func (m *FilesMetadataDTO) AddFile(localPath string, storagePath string, mTime time.Time, initialUplTS time.Time,
	meta RelFileMetadata, fileMode int64, isSkipped bool) {
	m.Files[localPath] = BackupFileDesc{
		StoragePath:     storagePath,
		RelNameMd5:      meta.RelNameMd5,
		IsSkipped:       isSkipped,
		MTime:           mTime,
		Kind:            meta.Kind,
		BlockID:         meta.BlockID,
		FileMode:        fileMode,
		InitialUploadTS: initialUplTS,
	}
}
