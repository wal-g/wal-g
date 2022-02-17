package greenplum

import (
	"os"
	"strconv"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DeleteHandler struct {
	internal.DeleteHandler
	permanentBackups map[string]bool
}

func NewDeleteHandler(folder storage.Folder) (*DeleteHandler, error) {
	backupObjects, err := internal.FindBackupObjects(folder)
	if err != nil {
		return nil, err
	}

	// todo better lessfunc
	gpLessFunc := func(obj1, obj2 storage.Object) bool {
		return obj1.GetLastModified().Before(obj2.GetLastModified())
	}

	permanentBackups := internal.FindPermanentBackups(folder, NewGenericMetaFetcher())
	isPermanentFunc := func(obj storage.Object) bool {
		return internal.IsPermanent(obj.GetName(), permanentBackups, BackupNameLength)
	}

	isIgnoredFunc := func(obj storage.Object) bool {
		// Remove only the basebackups folder objects, do not touch the segments folders.
		// WAL-G deals with them separately.
		objectName := obj.GetName()
		return !strings.HasPrefix(objectName, utility.BaseBackupPath)
	}

	return &DeleteHandler{
		DeleteHandler: *internal.NewDeleteHandler(
			folder,
			backupObjects,
			gpLessFunc,
			internal.IsPermanentFunc(isPermanentFunc),
			internal.IsIgnoredFunc(isIgnoredFunc),
		),
		permanentBackups: permanentBackups,
	}, nil
}

func (h *DeleteHandler) HandleDeleteBefore(args []string, confirmed bool) {
	modifier, beforeStr := internal.ExtractDeleteModifierFromArgs(args)

	target, err := h.FindTargetBefore(beforeStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetain(args []string, confirmed bool) {
	modifier, retentionStr := internal.ExtractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionStr)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetain(retentionCount, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetainAfter(args []string, confirmed bool) {
	modifier, retentionSir, afterStr := internal.ExtractDeleteRetainAfterModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionSir)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetainAfter(retentionCount, afterStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteEverything(args []string, confirmed bool) {
	h.DeleteHandler.HandleDeleteEverything(args, h.permanentBackups, confirmed)
}

func (h *DeleteHandler) DeleteBeforeTarget(target internal.BackupObject, confirmed bool) error {
	backup := NewBackup(h.Folder, target.GetBackupName())
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Deleting the segments backups...")
	// clean the segments
	for i := range sentinel.Segments {
		meta := &sentinel.Segments[i]
		tracelog.InfoLogger.Printf("Processing segment %d (backupId=%s)\n", meta.ContentID, meta.BackupID)

		segFolder := h.Folder.GetSubFolder(FormatSegmentStoragePrefix(meta.ContentID))
		permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(segFolder)

		segDeleteHandler, err := postgres.NewDeleteHandler(segFolder, permanentBackups, permanentWals, false)
		if err != nil {
			return err
		}

		pgBackup, err := backup.GetSegmentBackup(meta.BackupID, meta.ContentID)
		if err != nil {
			return err
		}

		segTarget, err := segDeleteHandler.FindTargetByName(pgBackup.Name)
		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("Running delete before target %s on segment %d\n",
			segTarget.GetBackupName(), meta.ContentID)
		err = segDeleteHandler.DeleteBeforeTarget(segTarget, confirmed)
		if err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Printf("Finished deleting the segments backups")

	return h.DeleteHandler.DeleteBeforeTarget(target, confirmed)
}
