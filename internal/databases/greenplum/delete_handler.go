package greenplum

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type DeleteArgs struct {
	Confirmed bool
	FindFull  bool
	Force     bool
}

type DeleteHandler struct {
	internal.DeleteHandler
	permanentBackups []string
	args             DeleteArgs
}

func NewDeleteHandler(ctx context.Context, folder storage.Folder, args DeleteArgs) (*DeleteHandler, error) {
	backupSentinelObjects, err := internal.GetBackupSentinelObjects(ctx, folder)
	if err != nil {
		return nil, err
	}

	backupObjects, err := makeBackupObjects(ctx, folder, backupSentinelObjects)
	if err != nil {
		return nil, err
	}

	if err := internal.ValidateBackupDependencies(backupObjects); err != nil {
		return nil, err
	}
	// todo better lessfunc
	gpLessFunc := func(obj1, obj2 storage.Object) bool {
		return obj1.GetLastModified().Before(obj2.GetLastModified())
	}

	permanentBackups, err := internal.GetPermanentBackupsChecked(ctx, folder.GetSubFolder(utility.BaseBackupPath),
		NewGenericMetaFetcher(), true)
	if err != nil {
		return nil, err
	}
	permanentBackupNames := make([]string, 0, len(permanentBackups))
	for name := range permanentBackups {
		permanentBackupNames = append(permanentBackupNames, name)
	}
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
		permanentBackups: permanentBackupNames,
		args:             args,
	}, nil
}

func (h *DeleteHandler) HandleDeleteBefore(ctx context.Context, args []string) {
	modifier, beforeStr := internal.ExtractDeleteModifierFromArgs(args)

	target, err := h.FindTargetBefore(beforeStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(ctx, target)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetain(ctx context.Context, args []string) {
	modifier, retentionStr := internal.ExtractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionStr)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetain(retentionCount, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(ctx, target)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetainAfter(ctx context.Context, args []string) {
	modifier, retentionSir, afterStr := internal.ExtractDeleteRetainAfterModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionSir)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetainAfter(retentionCount, afterStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(ctx, target)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteEverything(ctx context.Context, args []string) {
	h.DeleteHandler.HandleDeleteEverything(ctx, args, h.permanentBackups, h.args.Confirmed)
}

func (h *DeleteHandler) DeleteBeforeTarget(ctx context.Context, target internal.BackupObject) error {
	backupsBefore, err := listBackupsInOrder(ctx, h.Folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Deleting the segments backups...")
	err = h.dispatchDeleteCmd(ctx, target, SegDeleteBefore)
	if err != nil {
		return fmt.Errorf("failed to delete the segments backups: %w", err)
	}
	tracelog.InfoLogger.Printf("Finished deleting the segments backups")

	objFilter := func(object storage.Object) bool { return true }
	folderFilter := func(name string) bool { return strings.HasPrefix(name, utility.BaseBackupPath) }
	err = h.DeleteBeforeTargetWhere(ctx, target, h.args.Confirmed, objFilter, folderFilter)
	if err != nil {
		return err
	}
	backupsAfter, err := listBackupsInOrder(ctx, h.Folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}
	return ReassignSharedSizes(
		ctx,
		h.Folder,
		backupsWithChangedPredecessor(backupsBefore, backupsAfter),
		h.args.Confirmed,
	)
}

func (h *DeleteHandler) HandleDeleteTarget(ctx context.Context, targetSelector internal.BackupSelector) {
	target, err := h.FindTargetBySelector(ctx, targetSelector)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		// since we want to delete the target backup, we should fail if
		// we didn't find the requested backup for deletion
		tracelog.ErrorLogger.Fatal("Requested backup was not found")
	}
	tracelog.ErrorLogger.FatalOnError(h.deleteTargetCopies(ctx, target))
}

func (h *DeleteHandler) dispatchDeleteCmd(ctx context.Context, target internal.BackupObject, delType SegDeleteType) error {
	backup, err := NewBackupInStorage(ctx, h.Folder, target.GetBackupName(), multistorage.GetStorage(target))
	if err != nil {
		return err
	}
	sentinel, err := backup.GetSentinel(ctx)
	if err != nil {
		return fmt.Errorf("failed to load backup %s sentinel: %v", backup.Name, err)
	}

	deleteConcurrency, err := conf.GetMaxConcurrency(conf.GPDeleteConcurrency)
	if err != nil {
		tracelog.WarningLogger.Printf("config error: %v", err)
	}

	plans := make([]*SegDeletionPlan, len(sentinel.Segments))
	defer func() {
		for _, p := range plans {
			if p != nil {
				p.Close()
			}
		}
	}()
	preparation, preparationCtx := errgroup.WithContext(ctx)
	preparation.SetLimit(max(1, deleteConcurrency))
	for i, meta := range sentinel.Segments {
		preparation.Go(func() error {
			p, err := h.prepareSegmentDelete(preparationCtx, &backup, meta, delType)
			plans[i] = p
			return err
		})
	}
	if err := preparation.Wait(); err != nil {
		return err
	}
	execution, executionCtx := errgroup.WithContext(ctx)
	execution.SetLimit(max(1, deleteConcurrency))
	for _, p := range plans {
		if p != nil {
			execution.Go(func() error { return p.Execute(executionCtx) })
		}
	}
	return execution.Wait()
}

func (h *DeleteHandler) prepareSegmentDelete(ctx context.Context, backup *Backup, meta SegmentMetadata,
	delType SegDeleteType) (*SegDeletionPlan, error) {
	tracelog.InfoLogger.Printf("Preparing segment %d (backupId=%s)", meta.ContentID, meta.BackupID)
	handler, err := NewSegDeleteHandler(ctx, h.Folder, meta.ContentID, h.args, delType)
	if err != nil {
		return nil, err
	}
	segBackup, err := backup.GetSegmentBackup(ctx, meta.BackupID, meta.ContentID)
	if err != nil {
		if h.args.Force && ctx.Err() == nil {
			tracelog.ErrorLogger.Printf("Processing segment %d (backupId=%s): %v", meta.ContentID, meta.BackupID, err)
			return nil, nil
		}
		return nil, err
	}
	return handler.Prepare(ctx, segBackup)
}

// HandleDeleteGarbage delete outdated WAL archives and leftover backup files
func (h *DeleteHandler) HandleDeleteGarbage(ctx context.Context, args []string) error {
	backupsBefore, err := listBackupsInOrder(ctx, h.Folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}

	predicate := postgres.ExtractDeleteGarbagePredicate(args)
	backupSelector := internal.NewOldestNonPermanentSelector(NewGenericMetaFetcher())
	oldestBackup, err := backupSelector.Select(ctx, h.Folder)
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find any non-permanent backups in storage. Not doing anything.")
			return nil
		}
		return err
	}

	target, err := h.FindTargetByName(oldestBackup.Name)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Processing the segments...")
	err = h.dispatchDeleteCmd(ctx, target, SegDeleteBefore)
	if err != nil {
		return fmt.Errorf("failed to delete: %w", err)
	}
	tracelog.InfoLogger.Printf("Finished processing the segments backups")

	folderFilter := func(name string) bool { return strings.HasPrefix(name, utility.BaseBackupPath) }
	if err := h.DeleteBeforeTargetWhere(ctx, target, h.args.Confirmed, predicate, folderFilter); err != nil {
		return err
	}
	backupsAfter, err := listBackupsInOrder(ctx, h.Folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}
	return ReassignSharedSizes(
		ctx,
		h.Folder,
		backupsWithChangedPredecessor(backupsBefore, backupsAfter),
		h.args.Confirmed,
	)
}
