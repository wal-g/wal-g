package asm

import (
	"path/filepath"
	"strings"
)

func getOnlyWalName(filePath string) string {
	filePath = filepath.Base(filePath)
	return strings.TrimSuffix(filePath, filepath.Ext(filePath))
}

type ArchiveStatusManager interface {
	IsWalAlreadyUploaded(string) bool
	MarkWalUploaded(string) error
	UnmarkWalFile(string) error
}
