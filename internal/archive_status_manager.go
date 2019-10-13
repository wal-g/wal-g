package internal

import (
	"path/filepath"
	"strings"
)

func getOnlyWalName(filePath string) string {
	filePath = filepath.Base(filePath)
	return strings.TrimSuffix(filePath, filepath.Ext(filePath))
}

func isWalAlreadyUploaded(uploader *Uploader, walFilePath string) bool {
	walFilePath = getOnlyWalName(walFilePath)
	return uploader.ArchiveStatusManager.FileExists(walFilePath)
}

func markWalUploaded(uploader *Uploader, walFilePath string) error {
	walFilePath = getOnlyWalName(walFilePath)
	return uploader.ArchiveStatusManager.CreateFile(walFilePath)
}

func unmarkWalFile(uploader *Uploader, walFilePath string) error {
	walFilePath = getOnlyWalName(walFilePath)
	return uploader.ArchiveStatusManager.DeleteFile(walFilePath)
}
