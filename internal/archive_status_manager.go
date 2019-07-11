package internal

import (
	"path/filepath"
	"strings"
)

func getOnlyWalName(filename string) string {
	filename = filepath.Base(filename)
	return strings.TrimSuffix(filename, filepath.Ext(filename))
}

func isWalAlreadyUploaded(uploader *Uploader, walFilename string) bool {
	walFilename = getOnlyWalName(walFilename)
	return uploader.archiveStatusManager.FileExists(walFilename)
}

func markWalUploaded(uploader *Uploader, walFilename string) error {
	walFilename = getOnlyWalName(walFilename)
	return uploader.archiveStatusManager.CreateFile(walFilename)
}

func unmarkWalFile(uploader *Uploader, walFilename string) error {
	walFilename = getOnlyWalName(walFilename)
	return uploader.archiveStatusManager.DeleteFile(walFilename)
}
