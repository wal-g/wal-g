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
	Checksum        string         `json:"Checksum,omitempty"`
}

type AOFilesMetadataDTO struct {
	Files BackupAOFiles
	// UploadedSharedSize is the volume this backup uploaded to the shared aosegments/ storage:
	// Files smaller than WALG_GP_AOSEG_SIZE_THRESHOLD go into the regular tar balls and are not
	// part of it.
	//
	// It is what this backup uploaded, not what it owns: at the cluster level a backup can be
	// charged for the files of an older backup that was deleted while this one still reused them.
	UploadedSharedSize int64 `json:",omitempty"`
}

type BackupAOFiles map[string]*BackupAOFileDesc

func NewAOFilesMetadataDTO() *AOFilesMetadataDTO {
	return &AOFilesMetadataDTO{Files: make(BackupAOFiles)}
}

func (m *AOFilesMetadataDTO) addFile(key, storagePath string, mTime, initialUplTS time.Time, aoMeta AoRelFileMetadata,
	fileMode int64, isSkipped, isIncremented bool, checksum string) {
	m.Files[key] = &BackupAOFileDesc{
		StoragePath:     storagePath,
		IsSkipped:       isSkipped,
		IsIncremented:   isIncremented,
		MTime:           mTime,
		EOF:             aoMeta.eof,
		StorageType:     aoMeta.storageType,
		FileMode:        fileMode,
		ModCount:        aoMeta.modCount,
		InitialUploadTS: initialUplTS,
		Checksum:        checksum,
	}
}
