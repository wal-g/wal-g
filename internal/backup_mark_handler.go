package internal

func HandleBackupMark(uploader Uploader, backupName string, toPermanent bool, metaInteractor GenericMetaInteractor) {
	markHandler := NewBackupMarkHandler(metaInteractor, uploader.Folder())
	markHandler.MarkBackup(backupName, toPermanent)
}
