package greenplum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func loadDeletionBackup(ctx context.Context, root storage.Folder, name string) (internal.DeletionBackup, error) {
	var s BackupSentinelDto
	doc, err := readClusterDeletionMetadata(ctx, root, name, &s)
	if err != nil {
		return internal.DeletionBackup{}, err
	}
	b := internal.DeletionBackup{Name: name, Problem: doc.Problem, FinishTime: s.FinishTime, Metadata: s}
	b.Permanent, b.ProtectionKnown = doc.Bool("is_permanent")
	incErr := internal.ValidateDeletionIncrement(name, s.IncrementFrom, s.IncrementFullName, s.IncrementCount, false, false)
	b.Problem = errors.Join(b.Problem, incErr)
	b.DependenciesKnown = doc.Problem == nil && incErr == nil
	if b.DependenciesKnown && s.IncrementFrom != nil {
		b.Parent, b.Base = *s.IncrementFrom, *s.IncrementFullName
	}
	return b, nil
}

type deletionSegment struct {
	root     storage.Folder
	backups  map[string]internal.DeletionBackup
	targets  map[string]bool
	retained map[string]bool
	plan     *internal.DeletionPlan
}

func PlanRecoveryDeletion(ctx context.Context, root storage.Folder, destination string,
	names []string, logs bool) (*internal.DeletionPlan, error) {
	isBackupName := func(name string) bool { return strings.HasPrefix(name, "backup_") }
	pending, err := pendingRecoveryMappings(ctx, root)
	if err != nil {
		return nil, err
	}
	p, err := internal.NewDeletionPlan(ctx, root, destination, names, true, isBackupName, loadDeletionBackup, pending...)
	if err != nil {
		return p, err
	}
	state := greenplumDeletionPlanner{plan: p, segments: map[int]*deletionSegment{},
		mappings: map[string]map[int]string{}, retainedMappingsKnown: true}
	if err := state.resolveMappings(ctx); err != nil {
		return p, err
	}
	if err := state.prepareRetryMappings(ctx); err != nil {
		return p, err
	}
	if err := state.prepareCluster(ctx); err != nil {
		return p, err
	}
	ids := state.segmentIDs()
	for _, id := range ids {
		if err := state.prepareSegment(ctx, id); err != nil {
			return p, err
		}
	}
	for _, id := range ids {
		if err := state.prepareReclamation(ctx, id, logs); err != nil {
			return p, err
		}
	}
	return p, nil
}

type greenplumDeletionPlanner struct {
	plan                  *internal.DeletionPlan
	segments              map[int]*deletionSegment
	mappings              map[string]map[int]string
	retainedMappingsKnown bool
}

func (g *greenplumDeletionPlanner) segmentIDs() []int {
	ids := make([]int, 0, len(g.segments))
	for id := range g.segments {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func (g *greenplumDeletionPlanner) loadSegment(ctx context.Context, id int) (*deletionSegment, error) {
	if segment, ok := g.segments[id]; ok {
		return segment, nil
	}
	seg := &deletionSegment{root: g.plan.Root.GetSubFolder(FormatSegmentStoragePrefix(id)),
		backups: map[string]internal.DeletionBackup{}, targets: map[string]bool{}, retained: map[string]bool{}}
	objects, _, err := seg.root.GetSubFolder(utility.BaseBackupPath).ListFolder(ctx)
	if err != nil {
		return nil, err
	}
	for _, obj := range objects {
		key := strings.TrimLeft(obj.GetName(), "/")
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		name := strings.TrimSuffix(key, utility.SentinelSuffix)
		b, err := postgres.LoadDeletionBackup(ctx, seg.root, name)
		if err != nil {
			return nil, err
		}
		seg.backups[name] = b
	}
	g.segments[id] = seg
	return seg, nil
}

func (g *greenplumDeletionPlanner) resolveMappings(ctx context.Context) error {
	for name, b := range g.plan.Backups {
		s := b.Metadata.(BackupSentinelDto)
		g.mappings[name] = map[int]string{}
		if b.Problem != nil || len(s.Segments) == 0 {
			g.plan.Defer("segment mapping unavailable for cluster backup " + name)
			if !g.plan.Selected[name] {
				g.retainedMappingsKnown = false
			}
			continue
		}
		for _, meta := range s.Segments {
			if err := g.resolveSegment(ctx, name, &s, meta); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *greenplumDeletionPlanner) resolveSegment(ctx context.Context, name string, s *BackupSentinelDto, meta SegmentMetadata) error {
	if _, duplicate := g.mappings[name][meta.ContentID]; duplicate {
		return fmt.Errorf("duplicate segment %d in %s", meta.ContentID, name)
	}
	seg, err := g.loadSegment(ctx, meta.ContentID)
	if err != nil {
		return err
	}
	segName, err := resolveDeletionSegment(meta, seg.backups)
	if err != nil {
		return err
	}
	if segName == "" {
		g.plan.Defer(fmt.Sprintf("segment %d mapping unavailable for %s (legacy ID %s)", meta.ContentID, name, meta.BackupID))
		if !g.plan.Selected[name] {
			g.retainedMappingsKnown = false
		}
		return nil
	}
	g.mappings[name][meta.ContentID] = segName
	g.plan.Details = append(g.plan.Details, fmt.Sprintf("cluster=%s segment=%d backup=%s storage=%s",
		name, meta.ContentID, segName, g.plan.StorageName))
	if _, exists := seg.backups[segName]; !exists {
		b, err := postgres.LoadDeletionBackup(ctx, seg.root, segName)
		if err != nil {
			return err
		}
		seg.backups[segName] = b
	}
	if g.plan.Selected[name] {
		seg.targets[segName] = true
		return nil
	}
	seg.retained[segName] = true
	return extendSegmentProtection(ctx, g.plan.Root, s, meta, segName, seg)
}

func (g *greenplumDeletionPlanner) prepareCluster(ctx context.Context) error {
	var sidecars []string
	for _, name := range g.plan.TargetNames() {
		shared := !g.retainedMappingsKnown
		for _, survivor := range g.plan.Retained() {
			s := survivor.Metadata.(BackupSentinelDto)
			if s.RestorePoint != nil && *s.RestorePoint == name {
				shared = true
			}
		}
		if !shared {
			sidecars = append(sidecars, RestorePointMetadataFileName(name))
		} else {
			g.plan.Defer("restore point retained because its ownership is shared or unknown: " + name)
		}
	}
	g.plan.Finalizers = append(g.plan.Finalizers, g.finalizeRetryMappings)
	return g.plan.PrepareTargets(ctx, sidecars)
}

func (g *greenplumDeletionPlanner) prepareSegment(ctx context.Context, id int) error {
	seg := g.segments[id]
	if len(seg.targets) == 0 {
		return nil
	}
	if !g.retainedMappingsKnown {
		g.plan.Defer(fmt.Sprintf("segment %d retained: unknown surviving cluster mapping", id))
		return nil
	}
	var targets []string
	for name := range seg.targets {
		if seg.retained[name] {
			g.plan.Defer(fmt.Sprintf("segment %d backup %s is shared by a retained cluster backup", id, name))
			continue
		}
		targets = append(targets, name)
	}
	if len(targets) == 0 {
		return nil
	}
	loader := func(ctx context.Context, root storage.Folder, name string) (internal.DeletionBackup, error) {
		if b, ok := seg.backups[name]; ok {
			return b, nil
		}
		return postgres.LoadDeletionBackup(ctx, root, name)
	}
	isBackupName := func(name string) bool { return strings.HasPrefix(name, "base_") }
	sp, err := internal.NewDeletionPlan(ctx, seg.root, g.plan.StorageName, targets, true, isBackupName, loader)
	if err != nil {
		return err
	}
	for name := range sp.Selected {
		if seg.retained[name] {
			sp.Close()
			return fmt.Errorf("segment cascade would delete retained cluster dependency %d/%s", id, name)
		}
	}
	seg.plan = sp
	err = sp.PrepareTargets(ctx, nil)
	g.plan.Absorb(sp)
	return err
}

func (g *greenplumDeletionPlanner) prepareReclamation(ctx context.Context, id int, logs bool) error {
	sp := g.segments[id].plan
	if sp == nil {
		return nil
	}
	defer g.plan.Absorb(sp)
	if err := planSegmentShared(ctx, sp, ao.StoragePath, ao.FilesMetadataName, ao.KeySuffix); err != nil {
		return err
	}
	if err := planSegmentShared(ctx, sp, pax.StoragePath, pax.FilesMetadataName, pax.KeySuffix); err != nil {
		return err
	}
	if logs {
		if err := postgres.PlanRecoveryWALs(ctx, sp); err != nil {
			return err
		}
	}
	return nil
}

func resolveDeletionSegment(meta SegmentMetadata, catalog map[string]internal.DeletionBackup) (string, error) {
	if meta.BackupName != "" {
		if err := internal.ValidateRecoveryName(meta.BackupName); err != nil {
			return "", err
		}
		if !strings.HasPrefix(meta.BackupName, "base_") {
			return "", fmt.Errorf("invalid segment backup name %s", meta.BackupName)
		}
		if b, exists := catalog[meta.BackupName]; exists {
			id := segmentDeletionID(b)
			if id != "" && meta.BackupID != "" && id != meta.BackupID {
				return "", fmt.Errorf("conflicting segment name/ID for %s", meta.BackupName)
			}
		}
		return meta.BackupName, nil
	}
	if meta.BackupID == "" {
		return "", nil
	}
	match := ""
	for name, b := range catalog {
		if segmentDeletionID(b) == meta.BackupID {
			if match != "" {
				return "", fmt.Errorf("ambiguous segment backup ID %s", meta.BackupID)
			}
			match = name
		}
	}
	return match, nil
}

func segmentDeletionID(b internal.DeletionBackup) string {
	data, ok := b.Metadata.(postgres.DeletionBackupMetadata)
	if !ok {
		return ""
	}
	raw, _ := json.Marshal(data.Identity)
	var id struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &id)
	return id.ID
}

func extendSegmentProtection(ctx context.Context, root storage.Folder, cluster *BackupSentinelDto,
	meta SegmentMetadata, name string, seg *deletionSegment) error {
	b := seg.backups[name]
	data := b.Metadata.(postgres.DeletionBackupMetadata)
	if cluster.IsPermanent {
		b.Permanent, b.ProtectionKnown = true, true
	}
	if b.Permanent {
		lsn, err := postgres.ParseLSN(meta.RestorePointLSN)
		if meta.RestorePointLSN == "" && cluster.RestorePoint != nil {
			if err := internal.ValidateRecoveryName(*cluster.RestorePoint); err != nil {
				return err
			}
			var point RestorePointMetadata
			doc, readErr := internal.ReadDeletionMetadata(ctx, root.GetSubFolder(utility.BaseBackupPath),
				RestorePointMetadataFileName(*cluster.RestorePoint), &point)
			if readErr != nil {
				return readErr
			}
			if doc.Problem == nil && (point.Name == "" || point.Name == *cluster.RestorePoint) {
				lsn, err = postgres.ParseLSN(point.LsnBySegment[meta.ContentID])
			}
		}
		if err != nil || lsn < data.Meta.FinishLsn || lsn == ^postgres.LSN(0) {
			data.LogsKnown = false
		} else {
			data.Meta.FinishLsn = lsn + 1
		}
	}
	b.Metadata = data
	seg.backups[name] = b
	return nil
}

func planSegmentShared(ctx context.Context, p *internal.DeletionPlan, family, manifest, suffix string) error {
	base := p.Root.GetSubFolder(utility.BaseBackupPath)
	refs, err := internal.NewDeletionReferences()
	if err != nil {
		return err
	}
	defer refs.Close()
	for _, b := range p.Retained() {
		problem, err := walkSharedManifest(ctx, base, b.Name+"/"+manifest, refs.Add)
		if err != nil {
			return err
		}
		if problem != nil {
			p.Defer(family + " retained: unknown references for " + b.Name + ": " + problem.Error())
			return nil
		}
	}
	if err := refs.Seal(); err != nil {
		return err
	}
	return p.PlanObjectsChecked(ctx, "unreferenced "+family, base.GetSubFolder(family), func(obj storage.Object) (bool, error) {
		key := strings.TrimLeft(obj.GetName(), "/")
		if !strings.HasSuffix(key, suffix) || strings.Contains(key, "/") {
			return false, nil
		}
		live, err := refs.Contains(key)
		return !live, err
	})
}

func walkSharedManifest(ctx context.Context, base storage.Folder, key string,
	visit func(string) error) (problem, operational error) {
	r, err := base.ReadObject(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return &internal.UnavailableDeletionMetadataError{Key: key, Err: err}, nil
	}
	var visitorErr error
	problem, operational = internal.WalkDeletionManifest(r, func(key string, _ int64) error {
		visitorErr = visit(key)
		return visitorErr
	}, "")
	closeErr := r.Close()
	if visitorErr != nil {
		return nil, errors.Join(visitorErr, closeErr)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if errors.Is(operational, context.Canceled) || errors.Is(operational, context.DeadlineExceeded) {
		return nil, operational
	}
	if err := errors.Join(operational, closeErr); err != nil {
		return &internal.UnavailableDeletionMetadataError{Key: key, Err: err}, nil
	}
	return problem, nil
}
