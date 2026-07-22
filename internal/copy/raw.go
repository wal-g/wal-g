package copy

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

const defaultRawCopyConcurrency = 8

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

// Phase controls publication order. Commit objects advertise that a backup or
// recovery point is usable and therefore must be copied after its dependencies.
type Phase uint16

const (
	PayloadPhase Phase = iota
	MetadataPhase
	CommitPhase
)

// Entry describes one byte-preserving object transfer.
type Entry struct {
	SourcePath string
	TargetPath string
	Size       int64
	Phase      Phase
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

// SetPhase adjusts publication order for an entry already in the manifest.
func (p *Plan) SetPhase(targetPath string, phase Phase) error {
	targetPath = path.Clean(targetPath)
	entry, ok := p.entries[targetPath]
	if !ok {
		return fmt.Errorf("destination object %q is not in the copy manifest", targetPath)
	}
	entry.Phase = phase
	p.entries[targetPath] = entry
	return nil
}

// Plan is a complete, deduplicated copy manifest between two root folders.
type Plan struct {
	From storage.Folder
	To   storage.Folder

	source  map[string]storage.Object
	entries map[string]Entry
}

// NewPlan inventories the source once so planners can validate the full
// manifest before any destination object is written.
func NewPlan(ctx context.Context, from, to storage.Folder) (*Plan, error) {
	objects, err := storage.ListFolderRecursively(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("list source storage: %w", err)
	}

	source := make(map[string]storage.Object, len(objects))
	for _, object := range objects {
		source[path.Clean(object.GetName())] = object
	}

	return &Plan{From: from, To: to, source: source, entries: make(map[string]Entry)}, nil
}

// SourceObjects returns the immutable source inventory used by this plan.
func (p *Plan) SourceObjects() []storage.Object {
	objects := make([]storage.Object, 0, len(p.source))
	for _, object := range p.source {
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
// the same immutable content; their latest publication phase wins.
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
		if previous.Phase > entry.Phase {
			entry.Phase = previous.Phase
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
	if err := p.AddObject(sourceSentinel, targetSentinel, CommitPhase, false); err != nil {
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
		if a.Phase != b.Phase {
			return int(a.Phase) - int(b.Phase)
		}
		return strings.Compare(a.TargetPath, b.TargetPath)
	})
	return entries
}

// ExecuteRaw copies only missing immutable entries, rejects conflicting ones,
// and refreshes mutable metadata. No payload transformation is performed.
func ExecuteRaw(ctx context.Context, plan *Plan) error {
	targetObjects, err := storage.ListFolderRecursively(ctx, plan.To)
	if err != nil {
		return fmt.Errorf("list destination storage: %w", err)
	}
	target := make(map[string]storage.Object, len(targetObjects))
	for _, object := range targetObjects {
		target[path.Clean(object.GetName())] = object
	}

	byPhase := map[Phase][]Entry{}
	for _, entry := range plan.Entries() {
		if existing, ok := target[entry.TargetPath]; ok && !entry.Mutable {
			if existing.GetSize() != entry.Size {
				return fmt.Errorf("destination object %q conflicts with source: source size %d, destination size %d",
					entry.TargetPath, entry.Size, existing.GetSize())
			}
			tracelog.DebugLogger.Printf("Skipping existing object %q", entry.TargetPath)
			continue
		}
		byPhase[entry.Phase] = append(byPhase[entry.Phase], entry)
	}

	phases := make([]Phase, 0, len(byPhase))
	for phase := range byPhase {
		phases = append(phases, phase)
	}
	slices.Sort(phases)
	for _, phase := range phases {
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(defaultRawCopyConcurrency)
		for _, entry := range byPhase[phase] {
			group.Go(func() error { return copyRawObject(groupCtx, plan.From, plan.To, entry) })
		}
		if err := group.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func copyRawObject(ctx context.Context, from, to storage.Folder, entry Entry) error {
	if entry.inline != nil {
		if err := to.PutObject(ctx, entry.TargetPath, bytes.NewReader(entry.inline)); err != nil {
			return fmt.Errorf("write destination metadata %q: %w", entry.TargetPath, err)
		}
		tracelog.InfoLogger.Printf("Published metadata %q.", entry.TargetPath)
		return nil
	}
	reader, err := from.ReadObject(ctx, entry.SourcePath)
	if err != nil {
		return fmt.Errorf("read source object %q: %w", entry.SourcePath, err)
	}
	defer reader.Close()

	if err := to.PutObject(ctx, entry.TargetPath, reader); err != nil {
		return fmt.Errorf("write destination object %q: %w", entry.TargetPath, err)
	}
	tracelog.InfoLogger.Printf("Copied %q to %q without transformation.", entry.SourcePath, entry.TargetPath)
	return nil
}
