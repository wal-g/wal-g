package postgres

import (
	"context"
	"encoding/json"
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

// backupDeletionTarFiles covers local composers, including copied/rated parts.
// pg_control is uploaded separately from the composer. Streaming backups and
// backups without file metadata currently have no inventory and remain on the
// ordinary deletion path.
func backupDeletionTarFiles(meta FilesMetadataDto, extension string) []string {
	if len(meta.TarFileSets) == 0 {
		return nil
	}
	names := make([]string, 0, len(meta.TarFileSets)+1)
	for name := range meta.TarFileSets {
		names = append(names, name)
	}
	names = append(names, utility.AddFileExtension("pg_control.tar", extension))
	slices.Sort(names)
	return slices.Compact(names)
}

type NoListDeleteOptions struct {
	Before          string
	Retain          int
	Modifier        int
	After           string
	UseSentinelTime bool
	WalFrom         []string
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

type noListBackup struct {
	name string
	tars []string
}

type noListWALRange struct {
	timeline uint32
	first    uint64
}

// NoListDeletePlan keeps backup inventories and WAL ranges, not all WAL keys.
// The only listing is the shallow backup catalog used to establish retention.
type NoListDeletePlan struct {
	root      storage.Folder
	backups   []noListBackup
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
	if len(options.WalFrom) == 0 {
		return nil, fmt.Errorf("--no-list requires --wal-from to bound WAL enumeration")
	}
	catalog, objects, err := noListBackupCatalog(ctx, root)
	if err != nil {
		return nil, err
	}
	if err := selectNoListBackups(catalog, objects, options); err != nil {
		return nil, err
	}
	p := &NoListDeletePlan{root: root, retention: deletionWALRetention{
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
	for _, name := range catalog.TargetNames() {
		data := catalog.Backups[name].Metadata.(DeletionBackupMetadata)
		tars, err := deletionTarFiles(name, &data)
		if err != nil {
			return nil, err
		}
		p.backups = append(p.backups, noListBackup{name, tars})
	}
	if err := p.setWALRanges(options.WalFrom); err != nil {
		return nil, err
	}
	return p, nil
}

func noListBackupCatalog(ctx context.Context, root storage.Folder) (*internal.DeletionPlan, []internal.BackupObject, error) {
	sentinels, err := internal.GetBackupSentinelObjects(ctx, root)
	if err != nil {
		return nil, nil, err
	}
	catalog := &internal.DeletionPlan{Backups: map[string]internal.DeletionBackup{}, Selected: map[string]bool{}}
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

func deletionTarFiles(name string, data *DeletionBackupMetadata) ([]string, error) {
	var tars []string
	if data.Sentinel.FilesMetadataDisabled || len(data.TarFiles) == 0 {
		return nil, fmt.Errorf("backup %s has no TarFiles inventory; use ordinary deletion", name)
	}
	if err := json.Unmarshal(data.TarFiles, &tars); err != nil || len(tars) == 0 {
		return nil, fmt.Errorf("backup %s has an invalid TarFiles inventory", name)
	}
	for _, tar := range tars {
		if tar == "" || tar == "." || tar == ".." || path.Base(tar) != tar || strings.ContainsAny(tar, "\\\x00") ||
			!strings.Contains(tar, ".tar") {
			return nil, fmt.Errorf("invalid TarFiles entry %q in %s", tar, name)
		}
	}
	return tars, nil
}

func (p *NoListDeletePlan) setWALRanges(from []string) error {
	seen := map[uint32]bool{}
	for _, name := range from {
		timeline, first, err := parseDeletionWALFilename(name, p.retention.segmentSize)
		if err != nil || name != strings.ToUpper(name) {
			return fmt.Errorf("invalid --wal-from %q", name)
		}
		if !p.retention.knownTimelines[timeline] {
			return fmt.Errorf("--wal-from timeline %08X has no retained backup", timeline)
		}
		if seen[timeline] || first >= p.retention.cutoff {
			return fmt.Errorf("duplicate timeline or empty WAL range in --wal-from %s", name)
		}
		seen[timeline] = true
		p.ranges = append(p.ranges, noListWALRange{timeline, first})
	}
	return nil
}

func (p *NoListDeletePlan) Execute(ctx context.Context, confirmed bool) error {
	for _, backup := range p.backups {
		tracelog.InfoLogger.Printf("No-list deletion: backup=%s tar_keys=%d", backup.name, len(backup.tars))
	}
	for _, r := range p.ranges {
		tracelog.InfoLogger.Printf("No-list WAL range: timeline=%08X segments=[%d,%d), permanent intervals excluded",
			r.timeline, r.first, p.retention.cutoff)
	}
	if !confirmed {
		tracelog.InfoLogger.Println("Dry run; pass --confirm to delete the planned keys")
		return nil
	}
	base := p.root.GetSubFolder(utility.BaseBackupPath)
	batch := noListDeleteBatch{folder: base}
	for _, backup := range p.backups {
		if err := batch.add(ctx, backup.name+utility.SentinelSuffix); err != nil {
			return err
		}
	}
	if err := batch.flush(ctx); err != nil {
		return err
	}
	// Confirm unpublication before removing any payload or WAL.
	for _, backup := range p.backups {
		exists, err := base.Exists(ctx, backup.name+utility.SentinelSuffix)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("sentinel deletion unconfirmed: %s", backup.name)
		}
	}
	if err := p.deleteBackupPayload(ctx, &batch); err != nil {
		return err
	}
	batch.folder = p.root.GetSubFolder(utility.WalPath)
	if err := p.walkWALKeys(ctx, func(key string) error { return batch.add(ctx, key) }); err != nil {
		return err
	}
	return batch.flush(ctx)
}

func (p *NoListDeletePlan) deleteBackupPayload(ctx context.Context, batch *noListDeleteBatch) error {
	for _, backup := range p.backups {
		for _, tar := range backup.tars {
			if err := batch.add(ctx, internal.GetBackupTarPath(backup.name, tar)); err != nil {
				return err
			}
		}
		// Keep metadata available for recovery until this backup's tar deletions succeed.
		if err := batch.flush(ctx); err != nil {
			return err
		}
		for _, key := range []string{getFilesMetadataPath(backup.name), backup.name + "/" + utility.MetadataFileName,
			internal.JournalPrefix + backup.name} {
			if err := batch.add(ctx, key); err != nil {
				return err
			}
		}
		if err := batch.flush(ctx); err != nil {
			return err
		}
	}
	return nil
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
