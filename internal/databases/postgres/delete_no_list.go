package postgres

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type NoListDeleteOptions struct {
	Before          string
	Retain          int
	Modifier        int
	After           string
	UseSentinelTime bool
}

func (o NoListDeleteOptions) target(h *internal.DeleteHandler) (internal.BackupObject, error) {
	if o.Before != "" {
		return h.FindTargetBefore(o.Before, o.Modifier)
	}
	if o.Retain <= 0 {
		return nil, fmt.Errorf("positive retention count or a before boundary is required")
	}
	if o.After != "" {
		return h.FindTargetRetainAfter(o.Retain, o.After, o.Modifier)
	}
	return h.FindTargetRetain(o.Retain, o.Modifier)
}

type noListWALRange struct {
	timeline uint32
	first    uint64
}

// NoListDeletePlan derives WAL keys from existing backup metadata. Listings are
// limited to the shallow backup catalog and the selected backup subtrees;
// payload keys are spooled to disk and WAL keys are generated during execution.
type NoListDeletePlan struct {
	root      storage.Folder
	payload   *internal.DeletionPlan
	ranges    []noListWALRange
	retention deletionWALRetention
}

func PlanDeleteWithoutListing(ctx context.Context, root storage.Folder, options NoListDeleteOptions) (*NoListDeletePlan, error) {
	if err := multistorage.EnsureSingleStorageIsUsed(root); err != nil {
		return nil, err
	}
	if root.GetVersioningEnabled(ctx) {
		return nil, fmt.Errorf("--no-list does not support versioned storage")
	}
	catalog, objects, err := noListBackupCatalog(ctx, root)
	if err != nil {
		return nil, err
	}
	if err := selectNoListBackups(catalog, objects, options); err != nil {
		return nil, err
	}
	p := &NoListDeletePlan{root: root, payload: catalog, retention: deletionWALRetention{
		knownTimelines: map[uint32]bool{}, segmentSize: WalSegmentSize,
	}}
	for _, backup := range catalog.Retained() {
		if err := p.retention.retainBackup(catalog, backup); err != nil {
			return nil, err
		}
	}
	if !p.retention.hasCutoff {
		return nil, fmt.Errorf("--no-list requires an ordinary retained backup as the WAL boundary")
	}
	if err := p.setWALRanges(catalog); err != nil {
		return nil, err
	}
	if err := p.planBackupPayload(ctx); err != nil {
		p.Close()
		return nil, err
	}
	p.payload.Before = append(p.payload.Before, p.deleteSentinels)
	return p, nil
}

func (p *NoListDeletePlan) Close() { p.payload.Close() }

func noListBackupCatalog(ctx context.Context, root storage.Folder) (*internal.DeletionPlan, []internal.BackupObject, error) {
	sentinels, err := internal.GetBackupSentinelObjects(ctx, root)
	if err != nil {
		return nil, nil, err
	}
	catalog := &internal.DeletionPlan{Root: root, Backups: map[string]internal.DeletionBackup{}, Selected: map[string]bool{}}
	objects := make([]internal.BackupObject, 0, len(sentinels))
	for _, object := range sentinels {
		name := strings.TrimSuffix(strings.TrimLeft(object.GetName(), "/"), utility.SentinelSuffix)
		if err := internal.ValidateRecoveryName(name); err != nil {
			return nil, nil, err
		}
		if regexpPgBackupName.FindString(name) != name {
			return nil, nil, fmt.Errorf("unsupported PostgreSQL backup name %q", name)
		}
		backup, err := LoadDeletionBackup(ctx, root, name)
		if err != nil {
			return nil, nil, err
		}
		if backup.Problem != nil || !backup.ProtectionKnown || !backup.DependenciesKnown {
			return nil, nil, fmt.Errorf("cannot establish retention for %s: %v", name, backup.Problem)
		}
		catalog.Backups[name] = backup
		objects = append(objects, newBackupObject(backup.Base, backup.Parent, backup.Parent == "",
			object.GetLastModified(), object, multistorage.GetStorage(object)))
	}
	return catalog, objects, internal.ValidateBackupDependencies(objects)
}

func selectNoListBackups(catalog *internal.DeletionPlan, objects []internal.BackupObject, options NoListDeleteOptions) error {
	less := timelineAndSegmentNoLess
	if options.UseSentinelTime {
		times := make(map[string]time.Time, len(catalog.Backups))
		for name, backup := range catalog.Backups {
			start := backup.Metadata.(DeletionBackupMetadata).Meta.StartTime
			if start.IsZero() {
				return fmt.Errorf("missing backup start time for %s", name)
			}
			times[name] = start
		}
		less = makeLessFunc(times)
		for i, object := range objects {
			b := object.(BackupObject)
			b.creationTime = times[b.BackupName]
			objects[i] = b
		}
	}
	h := internal.NewDeleteHandler(nil, objects, less)
	target, err := options.target(h)
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("no retention boundary found")
	}
	if !target.IsFullBackup() {
		return fmt.Errorf("retention boundary %s is incremental; use FIND_FULL", target.GetBackupName())
	}
	for _, object := range objects {
		name := object.GetBackupName()
		if less(object, target) && !catalog.Backups[name].Permanent {
			catalog.Selected[name] = true
		}
	}
	for _, backup := range catalog.Retained() {
		for parent := backup.Parent; parent != ""; parent = catalog.Backups[parent].Parent {
			if catalog.Selected[parent] {
				return fmt.Errorf("retained backup %s depends on deletion target %s", backup.Name, parent)
			}
		}
	}
	return nil
}

func (p *NoListDeletePlan) setWALRanges(catalog *internal.DeletionPlan) error {
	// Validate selected backups with the same identity/segment-size checks as
	// retained backups, without changing the actual retention boundary.
	validation := deletionWALRetention{knownTimelines: map[uint32]bool{},
		segmentSize: p.retention.segmentSize, systemID: p.retention.systemID}
	starts := map[uint32]uint64{}
	for _, name := range catalog.TargetNames() {
		backup := catalog.Backups[name]
		if err := validation.retainBackup(catalog, backup); err != nil {
			return err
		}
		data := backup.Metadata.(DeletionBackupMetadata)
		timeline, _ := ParseTimelineFromBackupName(name) // Validated above.
		first := uint64(data.Meta.StartLsn-1) / p.retention.segmentSize
		if previous, ok := starts[timeline]; !ok || first < previous {
			starts[timeline] = first
		}
	}
	for timeline, first := range starts {
		if first < p.retention.cutoff {
			p.retention.knownTimelines[timeline] = true
			p.ranges = append(p.ranges, noListWALRange{timeline, first})
		}
	}
	slices.SortFunc(p.ranges, func(a, b noListWALRange) int { return cmp.Compare(a.timeline, b.timeline) })
	return nil
}

func (p *NoListDeletePlan) planBackupPayload(ctx context.Context) error {
	base := p.root.GetSubFolder(utility.BaseBackupPath)
	payload, err := p.payload.NewStage("selected backup payload", base)
	if err != nil {
		return err
	}
	metadata, err := p.payload.NewStage("backup metadata and journals", base)
	if err != nil {
		return err
	}
	for _, name := range p.payload.TargetNames() {
		// A single walk per selected backup also covers old/streaming backups,
		// disabled file metadata, non-contiguous tar parts and unknown sidecars.
		err := storage.WalkObjects(ctx, base.GetSubFolder(name), func(object storage.Object) error {
			key := name + "/" + strings.TrimLeft(object.GetName(), "/")
			stage := payload
			if key == getFilesMetadataPath(name) || key == path.Join(name, utility.MetadataFileName) {
				stage = metadata
			}
			return stage.Add(storage.NewLocalObjectWithVersion(key, object.GetLastModified(), object.GetSize(),
				object.GetVersionID(), object.GetAdditionalInfo()))
		})
		if err != nil {
			return fmt.Errorf("list selected backup %s: %w", name, err)
		}
		if err := metadata.Add(storage.NewLocalObject(internal.JournalPrefix+name, time.Time{}, 0)); err != nil {
			return err
		}
	}
	return nil
}

func (p *NoListDeletePlan) deleteSentinels(ctx context.Context) error {
	base := p.root.GetSubFolder(utility.BaseBackupPath)
	batch := noListDeleteBatch{folder: base}
	for _, name := range p.payload.TargetNames() {
		if err := batch.add(ctx, name+utility.SentinelSuffix); err != nil {
			return err
		}
	}
	if err := batch.flush(ctx); err != nil {
		return err
	}
	// Confirm unpublication before removing any payload or WAL.
	for _, name := range p.payload.TargetNames() {
		exists, err := base.Exists(ctx, name+utility.SentinelSuffix)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("sentinel deletion unconfirmed: %s", name)
		}
	}
	return nil
}

func (p *NoListDeletePlan) Execute(ctx context.Context, confirmed bool) error {
	for _, r := range p.ranges {
		tracelog.InfoLogger.Printf("No-list WAL range: timeline=%08X segments=[%d,%d), permanent intervals excluded",
			r.timeline, r.first, p.retention.cutoff)
	}
	if err := p.payload.Execute(ctx, confirmed); err != nil {
		return err
	}
	if !confirmed {
		return nil
	}
	batch := noListDeleteBatch{folder: p.root.GetSubFolder(utility.WalPath)}
	if err := p.walkWALKeys(ctx, func(key string) error { return batch.add(ctx, key) }); err != nil {
		return err
	}
	return batch.flush(ctx)
}

// Include historical codecs even when their decoders are excluded by build tags.
var noListWALExtensions = []string{"", "lz4", "lzma", "lzo", "br", "zst", "nop", "gzip", "gz"}

func (p *NoListDeletePlan) walkWALKeys(ctx context.Context, visit func(string) error) error {
	eligible := func(key string) error {
		if p.retention.eligible(storage.NewLocalObject(key, time.Time{}, 0)) {
			return visit(key)
		}
		return nil
	}
	for _, r := range p.ranges {
		for number := r.first; number < p.retention.cutoff; number++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			perLog := uint64(1<<32) / p.retention.segmentSize
			wal := fmt.Sprintf("%08X%08X%08X", r.timeline, number/perLog, number%perLog)
			if err := walkNoListSegmentKeys(wal, number, eligible); err != nil {
				return err
			}
		}
	}
	return nil
}

func walkNoListSegmentKeys(wal string, number uint64, visit func(string) error) error {
	names := []string{wal, wal + PartFilenameSuffix}
	metadata := []string{wal + ".json", "wal_metadata/" + wal + ".json"}
	if number%16 == 0 {
		names = append(names, wal+DeltaFilenameSuffix)
		metadata = append(metadata, wal[:23]+".json", "wal_metadata/"+wal[:23]+".json")
	}
	for _, name := range names {
		for _, extension := range noListWALExtensions {
			if err := visit(utility.AddFileExtension(name, extension)); err != nil {
				return err
			}
		}
	}
	for _, key := range metadata {
		if err := visit(key); err != nil {
			return err
		}
	}
	return nil
}

type noListDeleteBatch struct {
	folder  storage.Folder
	objects []storage.Object
}

func (b *noListDeleteBatch) add(ctx context.Context, key string) error {
	b.objects = append(b.objects, storage.NewLocalObject(key, time.Time{}, 0))
	if len(b.objects) == 500 {
		return b.flush(ctx)
	}
	return nil
}

func (b *noListDeleteBatch) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(b.objects) == 0 {
		return nil
	}
	if err := b.folder.DeleteObjects(ctx, b.objects); err != nil {
		return err
	}
	b.objects = b.objects[:0]
	return nil
}
