package postgres

import (
	"time"
)

const AOFilesMetadataName = "ao_files_metadata.json"

// getAOFilesMetadataPath returns AO files metadata storage path.
func getAOFilesMetadataPath(backupName string) string {
	return backupName + "/" + AOFilesMetadataName
}

type BackupAOFileDesc struct {
	StoragePath string         `json:"StoragePath"`
	IsSkipped   bool           `json:"IsSkipped"`
	MTime       time.Time      `json:"MTime"`
	StorageType RelStorageType `json:"StorageType"`
	EOF         int64          `json:"EOF"`
	Compressor  string         `json:"Compressor,omitempty"`
	FileMode    int64          `json:"FileMode"`
}

type AOFilesMetadataDTO struct {
	Files BackupAOFiles
}

type BackupAOFiles map[string]BackupAOFileDesc

func NewAOFilesMetadataDTO() *AOFilesMetadataDTO {
	return &AOFilesMetadataDTO{Files: make(BackupAOFiles)}
}

func (m *AOFilesMetadataDTO) addFile(key, storagePath string, mTime time.Time, aoMeta AoRelFileMetadata,
	fileMode int64, isSkipped bool) {
	m.Files[key] = BackupAOFileDesc{
		StoragePath: storagePath,
		IsSkipped:   isSkipped,
		MTime:       mTime,
		EOF:         aoMeta.eof,
		StorageType: aoMeta.storageType,
		FileMode:    fileMode,
	}
}
