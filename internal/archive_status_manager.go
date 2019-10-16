package internal

import (
	"path/filepath"
	"strings"
)

func getOnlyWalName(filePath string) string {
	filePath = filepath.Base(filePath)
	return strings.TrimSuffix(filePath, filepath.Ext(filePath))
}

type ArchiveStatusManager interface {
	isWalAlreadyUploaded(string) bool
	markWalUploaded(string) error
	unmarkWalFile(string) error
}

