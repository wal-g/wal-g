package copy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/statistics"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

var compressionExtensions = map[string]struct{}{
	".br":   {},
	".gz":   {},
	".lz4":  {},
	".lzma": {},
	".lzo":  {},
	".zst":  {},
}

// StripCompressionExtension recognizes every WAL-G archive suffix regardless
// of the compression algorithms enabled by this build. Planning must be able
// to treat an opaque object name correctly without loading its decompressor.
func StripCompressionExtension(name string) string {
	extension := path.Ext(name)
	if _, ok := compressionExtensions[extension]; ok {
		return strings.TrimSuffix(name, extension)
	}
	return name
}

// Phase is a publication stage. Commit objects advertise that a backup or
// recovery point is usable and therefore must be copied after its dependencies.
type Phase uint8

const (
	PayloadPhase Phase = iota
	MetadataPhase
	BackupCommitPhase
	RecoveryMetadataPhase
	AggregateCommitPhase
	FinalCommitPhase
)

// Entry describes one byte-preserving object transfer.
type Entry struct {
	SourcePath string
	TargetPath string
	Size       int64
	Phase      Phase
	Sequence   int
	Mutable    bool
	inline     []byte
}

// AddInline replaces a manifest destination with small planner-generated
// plaintext metadata. Backup and archive payloads must always use AddObject.
func (p *Plan) AddInline(targetPath string, content []byte, phase Phase, mutable bool) {
	targetPath = path.Clean(targetPath)
	p.entries[targetPath] = Entry{
		TargetPath: targetPath,
		Size:       int64(len(content)),
		Phase:      phase,
		Mutable:    mutable,
		inline:     slices.Clone(content),
	}
}

// SetOrder adjusts publication order for an entry already in the manifest.
// Sequence orders entries within a stage, for example parent and child backup
// sentinels in an incremental chain.
func (p *Plan) SetOrder(targetPath string, phase Phase, sequence int) error {
	targetPath = path.Clean(targetPath)
	entry, ok := p.entries[targetPath]
	if !ok {
		return fmt.Errorf("destination object %q is not in the copy manifest", targetPath)
	}
	entry.Phase = phase
	entry.Sequence = sequence
	p.entries[targetPath] = entry
	return nil
}

// Plan is a complete, deduplicated copy manifest between two root folders.
type Plan struct {
	From storage.Folder
	To   storage.Folder

	scoped  bool
	listed  map[string]bool
	source  map[string]storage.Object
	entries map[string]Entry
}

// NewPlan inventories the source once so planners can validate the full
// manifest before any destination object is written.
func NewPlan(ctx context.Context, from, to storage.Folder) (*Plan, error) {
	p := NewScopedPlan(from, to)
	objects, err := storage.ListFolderRecursively(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("list source storage: %w", err)
	}
	for _, object := range objects {
		p.source[path.Clean(object.GetName())] = object
	}
	p.scoped = false
	return p, nil
}

// NewScopedPlan inventories only explicitly requested backup dependencies.
func NewScopedPlan(from, to storage.Folder) *Plan {
	return &Plan{From: from, To: to, scoped: true,
		listed: make(map[string]bool), source: make(map[string]storage.Object), entries: make(map[string]Entry)}
}

// InventoryPrefix is for payloads whose complete key set is not recorded in
// metadata. Never pass a storage root when planning one selected backup.
func (p *Plan) InventoryPrefix(ctx context.Context, prefix string) error {
	if !p.scoped {
		return nil
	}
	prefix = path.Clean(strings.TrimSuffix(prefix, "/"))
	if err := validateSourcePath(prefix); err != nil {
		return err
	}
	if p.listed[prefix] {
		return nil
	}
	tracelog.InfoLogger.Printf("Listing selected copy source prefix %q", prefix)
	folder := p.From.GetSubFolder(prefix)
	if err := storage.WalkObjects(ctx, folder, func(object storage.Object) error {
		name := path.Join(prefix, object.GetName())
		p.source[name] = storage.NewLocalObjectWithVersion(name, object.GetLastModified(), object.GetSize(),
			object.GetVersionID(), object.GetAdditionalInfo())
		return nil
	}); err != nil {
		return fmt.Errorf("list source prefix %q: %w", prefix, err)
	}
	p.listed[prefix] = true
	return nil
}

// InventoryFolder reads only direct children, for backup/restore-point names.
func (p *Plan) InventoryFolder(ctx context.Context, prefix string) error {
	if err := validateSourcePath(path.Clean(prefix)); err != nil {
		return err
	}
	objects, _, err := p.From.GetSubFolder(prefix).ListFolder(ctx)
	if err != nil {
		return err
	}
	for _, object := range objects {
		name := path.Join(prefix, object.GetName())
		p.source[name] = storage.NewLocalObjectWithVersion(name, object.GetLastModified(), object.GetSize(),
			object.GetVersionID(), object.GetAdditionalInfo())
	}
	return nil
}

// FindObject looks up a known source key without listing its parent.
func (p *Plan) FindObject(ctx context.Context, name string) (storage.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = path.Clean(name)
	if err := validateSourcePath(name); err != nil {
		return nil, err
	}
	if object, ok := p.source[name]; ok {
		return object, nil
	}
	object, err := p.From.StatObject(ctx, name)
	if err != nil {
		return nil, err
	}
	p.source[name] = object
	return object, nil
}

// InventoryBackup discovers only one backup payload and its completion marker.
func (p *Plan) InventoryBackup(ctx context.Context, basePath, name string) error {
	if err := p.InventoryPrefix(ctx, path.Join(basePath, name)); err != nil {
		return err
	}
	_, err := p.FindObject(ctx, path.Join(basePath, internal.SentinelNameFromBackup(name)))
	return err
}

// AddSourceObject validates a known source key before adding it to a scoped plan.
func (p *Plan) AddSourceObject(ctx context.Context, sourcePath, targetPath string, phase Phase, mutable bool) error {
	if _, err := p.FindObject(ctx, sourcePath); err != nil {
		return fmt.Errorf("required source object %q: %w", sourcePath, err)
	}
	return p.AddObject(sourcePath, targetPath, phase, mutable)
}

func validateSourcePath(name string) error {
	if name == "." || name == ".." || strings.HasPrefix(name, "../") || path.IsAbs(name) {
		return fmt.Errorf("invalid copy source path %q", name)
	}
	return nil
}

// SourceObjects returns the immutable source inventory used by this plan.
func (p *Plan) SourceObjects() []storage.Object {
	return p.SourceObjectsAt("")
}

// SourceObjectsAt avoids sorting unrelated payload/shared-file keys for each
// segment's WAL validation. This only reads the already inventoried objects.
func (p *Plan) SourceObjectsAt(prefix string) []storage.Object {
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	var objects []storage.Object
	for name, object := range p.source {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		objects = append(objects, object)
	}
	slices.SortFunc(objects, func(a, b storage.Object) int { return strings.Compare(a.GetName(), b.GetName()) })
	return objects
}

// BackupNames returns completed backups found in the standard base-backup folder.
func (p *Plan) BackupNames() []string {
	prefix := strings.TrimSuffix(utility.BaseBackupPath, "/") + "/"
	names := make([]string, 0)
	for name := range p.source {
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, utility.SentinelSuffix) {
			continue
		}
		relative := strings.TrimPrefix(name, prefix)
		if strings.Contains(relative, "/") {
			continue
		}
		names = append(names, strings.TrimSuffix(relative, utility.SentinelSuffix))
	}
	slices.Sort(names)
	return names
}

// ResolveBackupNames applies the shared empty/all and LATEST semantics.
func (p *Plan) ResolveBackupNames(ctx context.Context, requested string) ([]string, error) {
	if requested == "" {
		if p.scoped {
			if err := p.InventoryFolder(ctx, utility.BaseBackupPath); err != nil {
				return nil, err
			}
		}
		names := p.BackupNames()
		if len(names) == 0 {
			return nil, internal.NewNoBackupsFoundError()
		}
		return names, nil
	}
	backup, err := internal.GetBackupByName(ctx, requested, utility.BaseBackupPath, p.From)
	if err != nil {
		return nil, err
	}
	return []string{backup.Name}, nil
}

// AddObject adds one source object. Duplicate destination paths must describe
// the same immutable content; their latest publication order wins.
func (p *Plan) AddObject(sourcePath, targetPath string, phase Phase, mutable bool) error {
	sourcePath = path.Clean(sourcePath)
	targetPath = path.Clean(targetPath)
	object, ok := p.source[sourcePath]
	if !ok {
		return fmt.Errorf("required source object %q does not exist", sourcePath)
	}

	entry := Entry{SourcePath: sourcePath, TargetPath: targetPath, Size: object.GetSize(), Phase: phase, Mutable: mutable}
	if previous, ok := p.entries[targetPath]; ok {
		if previous.SourcePath != entry.SourcePath || previous.Size != entry.Size {
			return fmt.Errorf("multiple source objects map to destination %q", targetPath)
		}
		if compareEntryOrder(previous, entry) > 0 {
			entry.Phase = previous.Phase
			entry.Sequence = previous.Sequence
		}
		entry.Mutable = previous.Mutable || entry.Mutable
	}
	p.entries[targetPath] = entry
	return nil
}

// AddMatching adds every object accepted by match, preserving its path.
func (p *Plan) AddMatching(match func(string) bool, phase Phase) error {
	for name := range p.source {
		if match(name) {
			if err := p.AddObject(name, name, phase, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// AddBackup adds exactly one standard backup subtree and its stop sentinel.
// It deliberately does not use prefix-only matching, because backup names may
// share prefixes.
func (p *Plan) AddBackup(sourceName, targetName string) error {
	return p.AddBackupAt(utility.BaseBackupPath, sourceName, targetName)
}

// AddBackupAt adds a backup in a database-specific nested base-backup folder,
// such as a Greenplum segment subtree.
func (p *Plan) AddBackupAt(basePath, sourceName, targetName string) error {
	base := strings.TrimSuffix(basePath, "/")
	sourcePrefix := path.Join(base, sourceName) + "/"
	targetPrefix := path.Join(base, targetName) + "/"
	foundData := false
	for name := range p.source {
		if !strings.HasPrefix(name, sourcePrefix) {
			continue
		}
		foundData = true
		relative := strings.TrimPrefix(name, sourcePrefix)
		phase := PayloadPhase
		if relative == utility.MetadataFileName || relative == utility.StreamMetadataFileName {
			phase = MetadataPhase
		}
		if err := p.AddObject(name, targetPrefix+relative, phase, false); err != nil {
			return err
		}
	}

	sourceSentinel := path.Join(base, internal.SentinelNameFromBackup(sourceName))
	targetSentinel := path.Join(base, internal.SentinelNameFromBackup(targetName))
	if err := p.AddObject(sourceSentinel, targetSentinel, BackupCommitPhase, false); err != nil {
		return err
	}
	if !foundData {
		tracelog.WarningLogger.Printf("backup %q contains no objects below its data prefix", sourceName)
	}
	return nil
}

// Entries returns the stable manifest ordering used by tests and diagnostics.
func (p *Plan) Entries() []Entry {
	entries := make([]Entry, 0, len(p.entries))
	for _, entry := range p.entries {
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(a, b Entry) int {
		if order := compareEntryOrder(a, b); order != 0 {
			return order
		}
		return strings.Compare(a.TargetPath, b.TargetPath)
	})
	return entries
}

func compareEntryOrder(a, b Entry) int {
	if a.Phase < b.Phase {
		return -1
	}
	if a.Phase > b.Phase {
		return 1
	}
	if a.Sequence < b.Sequence {
		return -1
	}
	if a.Sequence > b.Sequence {
		return 1
	}
	return 0
}

type publicationOrder struct {
	phase    Phase
	sequence int
}

// ExecuteRaw copies only missing immutable entries, rejects conflicting ones,
// and refreshes mutable metadata. No payload transformation is performed.
func ExecuteRaw(ctx context.Context, plan *Plan) error {
	return Execute(ctx, plan, ExecuteOptions{})
}

func Execute(ctx context.Context, plan *Plan, options ExecuteOptions) error {
	retries, err := options.copyRetries()
	if err != nil {
		return err
	}
	options.MaxRetries = &retries
	concurrency, err := options.copyConcurrency()
	if err != nil {
		return err
	}
	byOrder, err := groupPendingEntries(ctx, plan, options)
	if err != nil {
		return err
	}

	var objects, size int64
	for _, entries := range byOrder {
		for _, entry := range entries {
			objects++
			size += entry.Size
		}
	}
	tracelog.InfoLogger.Printf("Copying %d objects, %d bytes, up to %d objects at a time.", objects, size, concurrency)

	orders := make([]publicationOrder, 0, len(byOrder))
	for order := range byOrder {
		orders = append(orders, order)
	}
	slices.SortFunc(orders, func(a, b publicationOrder) int {
		if a.phase < b.phase {
			return -1
		}
		if a.phase > b.phase {
			return 1
		}
		if a.sequence < b.sequence {
			return -1
		}
		if a.sequence > b.sequence {
			return 1
		}
		return 0
	})
	for _, order := range orders {
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(concurrency)
		for _, entry := range byOrder[order] {
			if groupCtx.Err() != nil {
				break
			}
			group.Go(func() error { return copyObject(groupCtx, plan.From, plan.To, entry, options) })
		}
		if err := group.Wait(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func groupPendingEntries(ctx context.Context, plan *Plan, options ExecuteOptions) (map[publicationOrder][]Entry, error) {
	entries := plan.Entries()
	concurrency, err := options.copyConcurrency()
	if err != nil {
		return nil, err
	}
	existing := make([]storage.Object, len(entries))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for i, entry := range entries {
		if entry.Mutable {
			continue
		}
		if groupCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			object, err := plan.To.StatObject(groupCtx, entry.TargetPath)
			var missing storage.ObjectNotFoundError
			if errors.As(err, &missing) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("stat destination object %q: %w", entry.TargetPath, err)
			}
			existing[i] = object
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	byOrder := map[publicationOrder][]Entry{}
	for i, entry := range entries {
		if object := existing[i]; object != nil {
			if object.GetSize() == entry.Size {
				tracelog.DebugLogger.Printf("Skipping existing object %q", entry.TargetPath)
				continue
			}
			if !options.OverwriteConflicts {
				return nil, fmt.Errorf("destination object %q conflicts with source: source size %d, destination size %d",
					entry.TargetPath, entry.Size, object.GetSize())
			}
			tracelog.WarningLogger.Printf("Will overwrite conflicting destination object %q: source size %d, destination size %d",
				entry.TargetPath, entry.Size, object.GetSize())
		}
		order := publicationOrder{phase: entry.Phase, sequence: entry.Sequence}
		byOrder[order] = append(byOrder[order], entry)
	}
	return byOrder, nil
}

func copyObject(ctx context.Context, from, to storage.Folder, entry Entry, options ExecuteOptions) error {
	statistics.WalgMetrics.UploadedFilesTotal.Inc()
	err := copyObjectWithoutMetrics(ctx, from, to, entry, options)
	if err != nil {
		statistics.WalgMetrics.UploadedFilesFailedTotal.Inc()
		if options.SkipErrorsFor != nil && ctx.Err() == nil &&
			!errors.Is(err, context.Canceled) &&
			options.SkipErrorsFor.MatchString(entry.SourcePath) {
			tracelog.WarningLogger.Printf("Skipping failed copy of %q: %v", entry.SourcePath, err)
			return nil
		}
	}
	return err
}

func copyObjectWithoutMetrics(ctx context.Context, from, to storage.Folder, entry Entry, options ExecuteOptions) error {
	retries, err := options.copyRetries()
	if err != nil {
		return err
	}
	var serverCopyErr error
	err = retryCopy(ctx, retries, copyRetryDelay, func(ctx context.Context) error {
		return copyObjectAttempt(ctx, from, to, entry, &options, &serverCopyErr)
	})
	if err != nil && serverCopyErr != nil && options.ServerSideCopyFallback {
		return errors.Join(serverCopyErr, err)
	}
	return err
}

func copyObjectAttempt(ctx context.Context, from, to storage.Folder, entry Entry,
	options *ExecuteOptions, serverCopyErr *error) error {
	if entry.inline != nil {
		if err := to.PutObject(ctx, entry.TargetPath, bytes.NewReader(entry.inline)); err != nil {
			return &transferError{"write destination metadata", to, err}
		}
		tracelog.InfoLogger.Printf("Published metadata %q (%d bytes).", entry.TargetPath, entry.Size)
		return nil
	}

	if options.UseServerSideCopy {
		if copier, ok := to.(storage.CrossFolderCopier); ok && copier.CanCopyFrom(from) {
			*serverCopyErr = copier.CopyObjectFrom(ctx, from, entry.SourcePath, entry.TargetPath, entry.Size)
			if *serverCopyErr == nil {
				tracelog.InfoLogger.Printf(
					"Copied %q to %q on the storage side (%d bytes).",
					entry.SourcePath, entry.TargetPath, entry.Size,
				)
				return nil
			}
			if ctx.Err() != nil || errors.Is(*serverCopyErr, context.Canceled) ||
				!options.ServerSideCopyFallback {
				return &transferError{"server-side copy", to, *serverCopyErr}
			}
			options.UseServerSideCopy = false
			tracelog.WarningLogger.Printf(
				"Server-side copy %q to %q failed, falling back to streaming: %v",
				entry.SourcePath, entry.TargetPath, *serverCopyErr)
		}
	}

	if err := streamObject(ctx, from, to, entry.SourcePath, entry.TargetPath, entry.Size, nil); err != nil {
		return fmt.Errorf("copy %q to %q: %w", entry.SourcePath, entry.TargetPath, err)
	}
	tracelog.InfoLogger.Printf(
		"Streamed %q to %q through WAL-G without transformation (%d bytes).",
		entry.SourcePath, entry.TargetPath, entry.Size,
	)
	return nil
}
