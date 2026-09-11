package asm

import (
	"path/filepath"
	"strings"
)

func GetOnlyWalName(filePath string) string {
	filePath = filepath.Base(filePath)
	return strings.TrimSuffix(filePath, filepath.Ext(filePath))
}

type ArchiveStatusManager interface {
	IsWalAlreadyUploaded(string) bool
	MarkWalUploaded(string) error
	UnmarkWalFile(string) error
	RenameReady(string) error
	// ListUploaded returns the names of WAL files marked as uploaded,
	// up to limit entries. A limit <= 0 means no limit.
	ListUploaded(limit int) ([]string, error)
}
