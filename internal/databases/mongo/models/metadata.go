package models

type NsInfo struct {
	Ns           string `bson:"ns"`
	StorageStats struct {
		TotalSize  int64 `bson:"totalSize"`
		WiredTiger struct {
			URI string `bson:"uri"`
		} `bson:"wiredTiger"`
		IndexDetails map[string]struct {
			URI string `bson:"uri"`
		} `bson:"indexDetails"`
	} `bson:"storageStats"`
}

type Paths struct {
	DBPath  string `json:"db_path"`
	TarPath string `json:"tar_path"`
}

type IndexInfo map[string]Paths

type CollectionInfo struct {
	Paths     `json:"paths"`
	IndexInfo `json:"index_info"`
}

type DBInfo map[string]CollectionInfo

type BackupRoutesInfo struct {
	Databases map[string]DBInfo `json:"databases"`
	Service   map[string]string `json:"service"`
}

func NewBackupRoutesInfo() *BackupRoutesInfo {
	return &BackupRoutesInfo{
		Databases: make(map[string]DBInfo),
		Service:   make(map[string]string),
	}
}
