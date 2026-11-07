package greenplum

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type SegDeleteType int

const (
	SegDeleteBefore SegDeleteType = iota
	SegDeleteTarget
)

type SegDeleteHandler interface {
	Prepare(ctx context.Context, backup SegBackup) (*SegDeletionPlan, error)
}

type SegDeletionPlan struct {
	shared       *internal.DeletionPlan
	deleteBackup func(context.Context) error
	confirmed    bool
}

func (p *SegDeletionPlan) Close() { p.shared.Close() }
func (p *SegDeletionPlan) Execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.deleteBackup(ctx); err != nil {
		return err
	}
	return p.shared.Execute(ctx, p.confirmed)
}

type SegDeleteBeforeHandler struct {
	*postgres.DeleteHandler
	contentID int
	args      DeleteArgs
}

func NewSegDeleteHandler(ctx context.Context, rootFolder storage.Folder, contentID int, args DeleteArgs, delType SegDeleteType,
) (SegDeleteHandler, error) {
	segFolder := rootFolder.GetSubFolder(FormatSegmentStoragePrefix(contentID))

	permanentBackups, permanentWals, err := getPermanentBackupsAndWalsChecked(ctx, rootFolder, contentID)
	if err != nil {
		return nil, err
	}

	segDeleteHandler, err := postgres.NewDeleteHandler(ctx, segFolder, permanentBackups, permanentWals, false)
	if err != nil {
		return nil, err
	}

	switch delType {
	case SegDeleteBefore:
		return &SegDeleteBeforeHandler{
			DeleteHandler: segDeleteHandler,
			contentID:     contentID,
			args:          args,
		}, nil

	case SegDeleteTarget:
		return &SegDeleteTargetHandler{
			DeleteHandler: segDeleteHandler,
			contentID:     contentID,
			args:          args,
		}, nil

	default:
		return nil, fmt.Errorf("unknown segment delete handler type")
	}
}

func (h SegDeleteBeforeHandler) Prepare(ctx context.Context, segBackup SegBackup) (*SegDeletionPlan, error) {
	segTarget, err := h.FindTargetByName(segBackup.Name)
	if err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Printf("Running delete before target %s on segment %d\n",
		segTarget.GetBackupName(), h.contentID)
	selectedByStorage := map[string][]string{}
	for _, name := range multistorage.UsedStorages(h.Folder) {
		selectedByStorage[name] = h.BackupNamesBefore(segTarget, name)
	}
	sharedPlan, err := prepareSharedCleanup(ctx, h.Folder, selectedByStorage)
	if err != nil {
		if sharedPlan != nil {
			sharedPlan.Close()
		}
		return nil, err
	}

	filterFunc := func(object storage.Object) bool { return true }
	folderFilter := func(folderPath string) bool {
		folderPath = strings.TrimLeft(folderPath, "/")
		aoSegFolderPrefix := path.Join(utility.BaseBackupPath, ao.StoragePath)
		paxFolderPrefix := path.Join(utility.BaseBackupPath, pax.StoragePath)
		return !strings.HasPrefix(folderPath, aoSegFolderPrefix) &&
			!strings.HasPrefix(folderPath, paxFolderPrefix)
	}
	return &SegDeletionPlan{shared: sharedPlan, confirmed: h.args.Confirmed,
		deleteBackup: func(ctx context.Context) error {
			err := h.DeleteBeforeTargetWhere(ctx, segTarget, h.args.Confirmed, filterFunc, folderFilter)
			if err != nil {
				return err
			}

			return nil
		}}, nil
}
func (h SegDeleteBeforeHandler) Delete(ctx context.Context, segBackup SegBackup) error {
	p, err := h.Prepare(ctx, segBackup)
	if err != nil {
		return err
	}
	defer p.Close()
	return p.Execute(ctx)
}

type SegDeleteTargetHandler struct {
	*postgres.DeleteHandler
	contentID int
	args      DeleteArgs
}

func (h SegDeleteTargetHandler) Prepare(ctx context.Context, segBackup SegBackup) (*SegDeletionPlan, error) {
	return h.prepareTargets(ctx, []string{segBackup.Name})
}

func (h SegDeleteTargetHandler) prepareTargets(ctx context.Context, names []string) (*SegDeletionPlan, error) {
	var targets []internal.BackupObject
	for _, name := range names {
		target, err := h.FindTargetByName(name)
		if err != nil {
			return nil, err
		}
		targets = append(targets, h.TargetCopies(target)...)
	}
	folderFilter := func(folderPath string) bool {
		folderPath = strings.TrimLeft(folderPath, "/")
		return !strings.HasPrefix(folderPath, ao.StoragePath) && !strings.HasPrefix(folderPath, pax.StoragePath)
	}
	backups, err := h.PrepareDeleteTargets(ctx, targets, h.args.FindFull, folderFilter)
	if err != nil {
		return nil, err
	}
	selected := map[string][]string{}
	for _, backup := range backups.Backups {
		selected[backup.GetStorage()] = append(selected[backup.GetStorage()], backup.GetBackupName())
	}
	shared, err := prepareSharedCleanup(ctx, h.Folder, selected)
	if err != nil {
		if shared != nil {
			shared.Close()
		}
		return nil, err
	}
	return &SegDeletionPlan{shared: shared, confirmed: h.args.Confirmed, deleteBackup: func(ctx context.Context) error {
		if err := backups.Execute(ctx, h.args.Confirmed); err != nil {
			return err
		}
		for destination, names := range selected {
			folder, err := multistorage.UseSpecificStorage(ctx, destination, h.Folder)
			if err != nil {
				return err
			}
			for _, name := range names {
				internal.DeleteJournalInfo(ctx, folder, name, utility.WalPath, h.args.Confirmed)
			}
		}
		return nil
	}}, nil
}
func (h SegDeleteTargetHandler) Delete(ctx context.Context, segBackup SegBackup) error {
	p, err := h.Prepare(ctx, segBackup)
	if err != nil {
		return err
	}
	defer p.Close()
	return p.Execute(ctx)
}

func prepareSharedCleanup(ctx context.Context, root storage.Folder, selectedByStorage map[string][]string) (*internal.DeletionPlan, error) {
	result := &internal.DeletionPlan{Root: root}
	for destination, names := range selectedByStorage {
		if len(names) == 0 {
			continue
		}
		selected, _, err := multistorage.PinStorage(ctx, root, destination)
		if err != nil {
			return result, err
		}
		p := &internal.DeletionPlan{Root: selected, StorageName: destination,
			Backups: map[string]internal.DeletionBackup{}, Selected: map[string]bool{}}
		for _, name := range names {
			p.Selected[name] = true
		}
		backups, err := internal.GetBackups(ctx, selected.GetSubFolder(utility.BaseBackupPath))
		if err != nil {
			return result, err
		}
		for _, b := range backups {
			p.Backups[b.BackupName] = internal.DeletionBackup{Name: b.BackupName}
		}
		err = planSegmentShared(ctx, p, ao.StoragePath, ao.FilesMetadataName, ao.KeySuffix)
		if err == nil {
			err = planSegmentShared(ctx, p, pax.StoragePath, pax.FilesMetadataName, pax.KeySuffix)
		}
		result.Absorb(p)
		if err != nil {
			return result, err
		}
	}
	if len(result.Unresolved) != 0 {
		result.Print()
		return result, &internal.IncompleteDeletionError{Reasons: result.Unresolved}
	}
	return result, nil
}

func GetPermanentBackupsAndWals(ctx context.Context, rootFolder storage.Folder, contentID int,
) (map[postgres.PermanentObject]bool, map[postgres.PermanentObject]bool) {
	backups, wals, err := getPermanentBackupsAndWalsChecked(ctx, rootFolder, contentID)
	tracelog.ErrorLogger.FatalOnError(err)
	return backups, wals
}

func getPermanentBackupsAndWalsChecked(ctx context.Context, root storage.Folder, contentID int,
) (map[postgres.PermanentObject]bool, map[postgres.PermanentObject]bool, error) {
	objects, err := internal.GetBackups(ctx, root.GetSubFolder(utility.BaseBackupPath))
	var empty internal.NoBackupsFoundError
	if err != nil && !errors.As(err, &empty) {
		return nil, nil, err
	}
	protected, wals := map[postgres.PermanentObject]bool{}, map[postgres.PermanentObject]bool{}
	for _, destination := range multistorage.UsedStorages(root) {
		selected, err := multistorage.UseSpecificStorage(ctx, destination, root)
		if err != nil {
			return nil, nil, err
		}
		seg, err := loadProtectedSegment(ctx, selected, contentID)
		if err != nil {
			return nil, nil, err
		}
		associated, err := associateProtectedSegment(ctx, selected, destination, contentID, objects, seg)
		if err != nil {
			return nil, nil, err
		}
		for name, b := range seg.backups {
			if !b.Permanent {
				continue
			}
			data := b.Metadata.(postgres.DeletionBackupMetadata)
			if !associated[name] || !data.LogsKnown {
				return nil, nil, fmt.Errorf("unknown restore point for permanent segment backup %s", name)
			}
			timeline, err := postgres.ParseTimelineFromBackupName(name)
			if err != nil {
				return nil, nil, err
			}
			first, last := postgres.NewWalSegmentNo(data.Meta.StartLsn-1), postgres.NewWalSegmentNo(data.Meta.FinishLsn-1)
			for segment := first; segment <= last; segment++ {
				wals[postgres.PermanentObject{Name: segment.GetFilename(timeline), StorageName: destination}] = true
			}
			protected[postgres.PermanentObject{Name: name, StorageName: destination}] = true
		}
	}
	return protected, wals, nil
}

func loadProtectedSegment(ctx context.Context, selected storage.Folder, contentID int) (*deletionSegment, error) {
	seg := &deletionSegment{root: selected.GetSubFolder(FormatSegmentStoragePrefix(contentID)), backups: map[string]internal.DeletionBackup{}}
	times, err := internal.GetBackups(ctx, seg.root.GetSubFolder(utility.BaseBackupPath))
	var empty internal.NoBackupsFoundError
	if err != nil && !errors.As(err, &empty) {
		return nil, err
	}
	for _, item := range times {
		b, err := postgres.LoadDeletionBackup(ctx, seg.root, item.BackupName)
		if err != nil {
			return nil, err
		}
		if !b.ProtectionKnown || b.Problem != nil {
			return nil, fmt.Errorf("unknown protection of segment backup %s: %v", b.Name, b.Problem)
		}
		seg.backups[b.Name] = b
	}
	return seg, nil
}

func associateProtectedSegment(ctx context.Context, selected storage.Folder, destination string, contentID int,
	objects []internal.BackupTime, seg *deletionSegment) (map[string]bool, error) {
	associated := map[string]bool{}
	for _, item := range objects {
		if item.StorageName != destination {
			continue
		}
		b, err := loadDeletionBackup(ctx, selected, item.BackupName)
		if err != nil {
			return nil, err
		}
		if b.Problem != nil || !b.ProtectionKnown {
			return nil, fmt.Errorf("unknown protection of cluster backup %s", b.Name)
		}
		cluster := b.Metadata.(BackupSentinelDto)
		for _, meta := range cluster.Segments {
			if meta.ContentID != contentID {
				continue
			}
			name, err := resolveDeletionSegment(meta, seg.backups)
			if err != nil {
				return nil, err
			}
			if name == "" {
				return nil, fmt.Errorf("unknown segment mapping for %s", b.Name)
			}
			if _, ok := seg.backups[name]; !ok {
				return nil, fmt.Errorf("missing mapped segment backup %s", name)
			}
			if err := extendSegmentProtection(ctx, selected, &cluster, meta, name, seg); err != nil {
				return nil, err
			}
			associated[name] = true
		}
	}
	return associated, nil
}
