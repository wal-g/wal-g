package greenplum

import (
	"time"
)

const AOFilesMetadataName = "ao_files_metadata.json"

// getAOFilesMetadataPath returns AO files metadata storage path.
func getAOFilesMetadataPath(backupName string) string {
	return backupName + "/" + AOFilesMetadataName
}

type BackupAOFileDesc struct {
	StoragePath     string         `json:"StoragePath"`
	IsSkipped       bool           `json:"IsSkipped"`
	IsIncremented   bool           `json:"IsIncremented,omitempty"`
	MTime           time.Time      `json:"MTime"`
	StorageType     RelStorageType `json:"StorageType"`
	EOF             int64          `json:"EOF"`
	ModCount        int64          `json:"ModCount,omitempty"`
	Compressor      string         `json:"Compressor,omitempty"`
	FileMode        int64          `json:"FileMode"`
	InitialUploadTS time.Time      `json:"InitialUploadTS,omitempty"`
}

type AOFilesMetadataDTO struct {
	Files BackupAOFiles
}

type BackupAOFiles map[string]BackupAOFileDesc

func NewAOFilesMetadataDTO() *AOFilesMetadataDTO {
	return &AOFilesMetadataDTO{Files: make(BackupAOFiles)}
}

func (m *AOFilesMetadataDTO) addFile(key, storagePath string, mTime, initialUplTS time.Time, aoMeta AoRelFileMetadata,
	fileMode int64, isSkipped, isIncremented bool) {
	m.Files[key] = BackupAOFileDesc{
		StoragePath:     storagePath,
		IsSkipped:       isSkipped,
		IsIncremented:   isIncremented,
		MTime:           mTime,
		EOF:             aoMeta.eof,
		StorageType:     aoMeta.storageType,
		FileMode:        fileMode,
		ModCount:        aoMeta.modCount,
		InitialUploadTS: initialUplTS,
	}
}
