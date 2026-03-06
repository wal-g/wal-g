package models

type CollStats struct {
	NS           string `bson:"ns"`
	StorageStats struct {
		TotalSize int64 `bson:"totalSize"`
	} `bson:"storageStats"`
}
