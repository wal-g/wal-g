package greenplum

import (
	"context"
	"fmt"
	"sort"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type clusterTargetCopy struct {
	root    storage.Folder
	backups *internal.TargetDeletionPlan
	before  []internal.BackupTime
}

type segmentTargets struct {
	root  storage.Folder
	id    int
	names map[string]bool
}

func (h *DeleteHandler) deleteTargetCopies(ctx context.Context, target internal.BackupObject) error {
	var copies []clusterTargetCopy
	groups := map[string]*segmentTargets{}
	for _, target := range h.TargetCopies(target) {
		destination, err := h.prepareClusterCopy(ctx, target, groups)
		if err != nil {
			return err
		}
		copies = append(copies, destination)
	}
	plans, err := h.prepareTargetSegments(ctx, groups)
	defer func() {
		for _, plan := range plans {
			if plan != nil {
				plan.Close()
			}
		}
	}()
	if err != nil {
		return err
	}
	concurrency, _ := conf.GetMaxConcurrency(conf.GPDeleteConcurrency)
	execution, executeCtx := errgroup.WithContext(ctx)
	execution.SetLimit(max(1, concurrency))
	for _, plan := range plans {
		execution.Go(func() error { return plan.Execute(executeCtx) })
	}
	if err := execution.Wait(); err != nil {
		return err
	}
	for _, destination := range copies {
		if err := destination.execute(ctx, h.args.Confirmed); err != nil {
			return err
		}
	}
	return nil
}

func (h *DeleteHandler) prepareClusterCopy(ctx context.Context, target internal.BackupObject,
	groups map[string]*segmentTargets) (clusterTargetCopy, error) {
	root, err := multistorage.UseSpecificStorage(ctx, target.GetStorage(), h.Folder)
	if err != nil {
		return clusterTargetCopy{}, err
	}
	handler, err := NewDeleteHandler(ctx, root, h.args)
	if err != nil {
		return clusterTargetCopy{}, err
	}
	selected, err := handler.FindTargetByName(target.GetBackupName())
	if err != nil {
		return clusterTargetCopy{}, err
	}
	plan, err := handler.PrepareDeleteTargets(ctx, []internal.BackupObject{selected}, h.args.FindFull, func(string) bool { return true })
	if err != nil {
		return clusterTargetCopy{}, err
	}
	before, err := listBackupsInOrder(ctx, root.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return clusterTargetCopy{}, err
	}
	for _, cluster := range plan.Backups {
		if err := addClusterSegmentTargets(ctx, root, cluster, groups); err != nil {
			return clusterTargetCopy{}, err
		}
	}
	return clusterTargetCopy{root, plan, before}, nil
}

func addClusterSegmentTargets(ctx context.Context, root storage.Folder, cluster internal.BackupObject,
	groups map[string]*segmentTargets) error {
	backup, err := NewBackupInStorage(ctx, root, cluster.GetBackupName(), cluster.GetStorage())
	if err != nil {
		return err
	}
	sentinel, err := backup.GetSentinel(ctx)
	if err != nil {
		return err
	}
	for _, meta := range sentinel.Segments {
		segment, err := backup.GetSegmentBackup(ctx, meta.BackupID, meta.ContentID)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s/%d", cluster.GetStorage(), meta.ContentID)
		if groups[key] == nil {
			groups[key] = &segmentTargets{root, meta.ContentID, map[string]bool{}}
		}
		groups[key].names[segment.Name] = true
	}
	return nil
}

func (h *DeleteHandler) prepareTargetSegments(ctx context.Context, groups map[string]*segmentTargets) ([]*SegDeletionPlan, error) {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	plans := make([]*SegDeletionPlan, len(keys))
	concurrency, _ := conf.GetMaxConcurrency(conf.GPDeleteConcurrency)
	preparation, prepareCtx := errgroup.WithContext(ctx)
	preparation.SetLimit(max(1, concurrency))
	for i, key := range keys {
		preparation.Go(func() error {
			group := groups[key]
			handler, err := NewSegDeleteHandler(prepareCtx, group.root, group.id, h.args, SegDeleteTarget)
			if err != nil {
				return err
			}
			names := make([]string, 0, len(group.names))
			for name := range group.names {
				names = append(names, name)
			}
			sort.Strings(names)
			plans[i], err = handler.(*SegDeleteTargetHandler).prepareTargets(prepareCtx, names)
			return err
		})
	}
	return plans, preparation.Wait()
}

func (destination clusterTargetCopy) execute(ctx context.Context, confirmed bool) error {
	if err := destination.backups.Execute(ctx, confirmed); err != nil {
		return err
	}
	after, err := listBackupsInOrder(ctx, destination.root.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}
	for _, backup := range destination.backups.Backups {
		DeleteClusterJournalInfo(ctx, destination.root, backup.GetBackupName(), confirmed)
	}
	return ReassignSharedSizes(ctx, destination.root, backupsWithChangedPredecessor(destination.before, after), confirmed)
}
