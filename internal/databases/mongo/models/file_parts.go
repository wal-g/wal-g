package models

type FilePartsInfo struct {
	Size     int64 `json:"size"`
	PartSize int64 `json:"part_size"`
}
