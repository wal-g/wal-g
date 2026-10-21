package models

type FilePartsInfo struct {
	PartSize int64            `json:"part_size"`
	Files    map[string]int64 `json:"files"`
}
