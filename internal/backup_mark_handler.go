package internal

import "context"

func HandleBackupMark(ctx context.Context, uploader Uploader, backupName string, toPermanent bool, metaInteractor GenericMetaInteractor) {
	markHandler := NewBackupMarkHandler(metaInteractor, uploader.Folder())
	markHandler.MarkBackup(ctx, backupName, toPermanent)
}
