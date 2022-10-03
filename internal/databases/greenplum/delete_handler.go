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
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DeleteArgs struct {
	Confirmed bool
	FindFull  bool
}

type DeleteHandler struct {
	internal.DeleteHandler
	permanentBackups map[string]bool
	args             DeleteArgs
}

func NewDeleteHandler(folder storage.Folder, args DeleteArgs) (*DeleteHandler, error) {
	backupSentinelObjects, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupObjects, err := makeBackupObjects(folder, backupSentinelObjects)
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

	return &DeleteHandler{
		DeleteHandler: *internal.NewDeleteHandler(
			folder,
			backupObjects,
			gpLessFunc,
			internal.IsPermanentFunc(isPermanentFunc),
		),
		permanentBackups: permanentBackups,
		args:             args,
	}, nil
}

func (h *DeleteHandler) HandleDeleteBefore(args []string) {
	modifier, beforeStr := internal.ExtractDeleteModifierFromArgs(args)

	target, err := h.FindTargetBefore(beforeStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetain(args []string) {
	modifier, retentionStr := internal.ExtractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionStr)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetain(retentionCount, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetainAfter(args []string) {
	modifier, retentionSir, afterStr := internal.ExtractDeleteRetainAfterModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionSir)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetainAfter(retentionCount, afterStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteEverything(args []string) {
	h.DeleteHandler.HandleDeleteEverything(args, h.permanentBackups, h.args.Confirmed)
}

func (h *DeleteHandler) DeleteBeforeTarget(target internal.BackupObject) error {
	tracelog.InfoLogger.Println("Deleting the segments backups...")
	err := h.dispatchDeleteCmd(target, SegDeleteBefore)
	if err != nil {
		return fmt.Errorf("failed to delete the segments backups: %w", err)
	}
	tracelog.InfoLogger.Printf("Finished deleting the segments backups")

	objFilter := func(object storage.Object) bool { return true }
	folderFilter := func(name string) bool { return strings.HasPrefix(name, utility.BaseBackupPath) }
	return h.DeleteHandler.DeleteBeforeTargetWhere(target, h.args.Confirmed, objFilter, folderFilter)
}

func (h *DeleteHandler) DeleteTarget(target internal.BackupObject) {
	tracelog.InfoLogger.Println("Deleting the segments backups...")
	err := h.dispatchDeleteCmd(target, SegDeleteTarget)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to delete the segments backups: %v", err)
	}
	tracelog.InfoLogger.Printf("Finished deleting the segments backups")

	h.DeleteHandler.HandleDeleteTarget(target, h.args.Confirmed, h.args.FindFull)
}

func (h *DeleteHandler) dispatchDeleteCmd(target internal.BackupObject, delType SegDeleteType) error {
	backup := NewBackup(h.Folder, target.GetBackupName())
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return fmt.Errorf("failed to load backup %s sentinel: %v", backup.Name, err)
	}

	errorGroup, _ := errgroup.WithContext(context.Background())

	deleteConcurrency, err := internal.GetMaxConcurrency(internal.GPDeleteConcurrency)
	if err != nil {
		tracelog.WarningLogger.Printf("config error: %v", err)
	}

	deleteSem := make(chan struct{}, deleteConcurrency)

	// clean the segments
	for i := range sentinel.Segments {
		meta := sentinel.Segments[i]
		tracelog.InfoLogger.Printf("Processing segment %d (backupId=%s)\n", meta.ContentID, meta.BackupID)

		segHandler, err := NewSegDeleteHandler(h.Folder, meta.ContentID, h.args, delType)
		if err != nil {
			return err
		}

		segBackup, err := backup.GetSegmentBackup(meta.BackupID, meta.ContentID)
		if err != nil {
			return err
		}

		errorGroup.Go(func() error {
			deleteSem <- struct{}{}
			deleteErr := segHandler.Delete(segBackup)
			<-deleteSem
			return deleteErr
		})
	}

	return errorGroup.Wait()
}
