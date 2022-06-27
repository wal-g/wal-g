package greenplum

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

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

	permanentBackups := internal.GetPermanentBackups(folder.GetSubFolder(utility.BaseBackupPath),
		NewGenericMetaFetcher())
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
	errorGroup, _ := errgroup.WithContext(context.Background())

	deleteConcurrency, err := internal.GetMaxConcurrency(internal.GPDeleteConcurrency)
	if err != nil {
		tracelog.WarningLogger.Printf("config error: %v", err)
	}

	deleteSem := make(chan struct{}, deleteConcurrency)

	// clean the segments
	for i := range sentinel.Segments {
		meta := sentinel.Segments[i]
		errorGroup.Go(func() error {
			deleteSem <- struct{}{}
			deleteErr := h.runDeleteOnSegment(backup, meta, confirmed)
			<-deleteSem
			return deleteErr
		})
	}

	err = errorGroup.Wait()
	if err != nil {
		return fmt.Errorf("failed to delete the segments backups: %w", err)
	}

	tracelog.InfoLogger.Printf("Finished deleting the segments backups")

	objFilter := func(object storage.Object) bool { return true }
	folderFilter := func(name string) bool { return strings.HasPrefix(name, utility.BaseBackupPath) }
	return h.DeleteHandler.DeleteBeforeTargetWhere(target, confirmed, objFilter, folderFilter)
}

func (h *DeleteHandler) runDeleteOnSegment(backup Backup, meta SegmentMetadata, confirmed bool) error {
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

	filterFunc := func(object storage.Object) bool {
		return !strings.HasSuffix(object.GetName(), postgres.AoSegSuffix)
	}
	folderFilter := func(string) bool { return true }
	err = segDeleteHandler.DeleteBeforeTargetWhere(segTarget, confirmed, filterFunc, folderFilter)
	if err != nil {
		return err
	}

	return cleanupAOSegments(segFolder, confirmed)
}

func cleanupAOSegments(segFolder storage.Folder, confirmed bool) error {
	aoSegFolder := segFolder.GetSubFolder(utility.BaseBackupPath).GetSubFolder(postgres.AoStoragePath)
	tracelog.InfoLogger.Printf("Cleaning up the AO segment objects")
	aoSegmentsToDelete, err := findAoSegmentsToDelete(aoSegFolder)
	if err != nil {
		return err
	}

	return aoSegFolder.DeleteObjects(aoSegmentsToDelete)
}

func findAoSegmentsToDelete(aoSegFolder storage.Folder) ([]string, error) {
	aoObjects, _, err := aoSegFolder.ListFolder()
	if err != nil {
		return nil, err
	}

	// we want to retain AO segments that are still referenced by some backups
	aoSegmentsToRetain := make(map[string]struct{})
	for _, obj := range aoObjects {
		if strings.HasSuffix(obj.GetName(), postgres.BackupRefSuffix) {
			// this should never fail, since slice len is always > 0
			referencedSegName := strings.SplitAfter(obj.GetName(), postgres.AoSegSuffix)[0]
			aoSegmentsToRetain[referencedSegName] = struct{}{}
		}
	}

	aoSegmentsToDelete := make([]string, 0)
	for _, obj := range aoObjects {
		if !strings.HasSuffix(obj.GetName(), postgres.AoSegSuffix) {
			// this is not an AO segment file, skip it
			tracelog.DebugLogger.Println("\tis not an AO segment file, will not delete: " + obj.GetName())
			continue
		}

		if _, ok := aoSegmentsToRetain[obj.GetName()]; ok {
			// this AO segment file is still referenced by some backup, skip it
			tracelog.DebugLogger.Println("\tis still referenced by some backups, will not delete: " + obj.GetName())
			continue
		}

		tracelog.InfoLogger.Println("\twill be deleted: " + obj.GetName())

		aoSegmentsToDelete = append(aoSegmentsToDelete, obj.GetName())
	}

	return aoSegmentsToDelete, nil
}
