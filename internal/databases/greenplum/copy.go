package greenplum

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

const DefaultCopyWALWaitTimeout = 5 * time.Minute

const (
	copyWaitInitialDelay         = 5 * time.Second
	copyWaitMaxDelay             = time.Minute
	restorePointFetchConcurrency = 16
)

// HandleCopy preserves the original exact-restore copy API.
func HandleCopy(ctx context.Context, fromConfigFile string, toConfigFile string, backupName string) {
	HandleCopyWithHistory(ctx, fromConfigFile, toConfigFile, backupName, false, DefaultCopyWALWaitTimeout)
}

// HandleCopyWithHistory copies specific or all backups and optionally extends
// each segment WAL stream through the latest cluster restore point.
func HandleCopyWithHistory(
	ctx context.Context,
	fromConfigFile, toConfigFile, backupName string,
	withHistory bool,
	walWaitTimeout time.Duration,
) {
	from, fromConfig, err := internal.StorageAndConfigFromFile(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, toConfig, err := internal.StorageAndConfigFromFile(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := buildCopyPlanWaiting(
		ctx, from.RootFolder(), to.RootFolder(), backupName, withHistory, walWaitTimeout, sleepContext)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copy.Execute(ctx, plan, copy.OptionsFromConfigs(fromConfig, toConfig)))
	tracelog.InfoLogger.Println("Success copy.")
}

type notPublishedError struct {
	object string
}

func (err notPublishedError) Error() string {
	return fmt.Sprintf("%s is not published in the source storage yet", err.object)
}

func buildCopyPlanWaiting(
	ctx context.Context,
	from, to storage.Folder,
	backupName string,
	withHistory bool,
	timeout time.Duration,
	sleep func(context.Context, time.Duration) error,
) (*copy.Plan, error) {
	deadline := time.Now().Add(timeout)
	delay := copyWaitInitialDelay
	for {
		plan, err := BuildCopyPlan(ctx, from, to, backupName, withHistory)
		var notPublished notPublishedError
		if !errors.As(err, &notPublished) {
			return plan, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if timeout > 0 {
				err = fmt.Errorf("gave up waiting %s: %w", timeout, err)
			}
			return nil, err
		}
		delay = min(delay, remaining)
		tracelog.InfoLogger.Printf("%v, listing the source again in %s", err, delay)
		if err := sleep(ctx, delay); err != nil {
			return nil, err
		}
		delay = min(2*delay, copyWaitMaxDelay)
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type greenplumCopyState struct {
	backupState    map[string]uint8
	backupDepths   map[string]int
	segmentState   map[string]uint8
	segmentDepths  map[string]int
	segmentBackups map[string]postgres.Backup

	walFolders    map[string]walFolderIndex
	filesMetadata map[string]struct{}
	restorePoints map[string]RestorePointMetadata
}

type walFolderIndex struct {
	archives map[string]struct{}
	segments map[postgres.WalSegmentNo][]string
}

func (index walFolderIndex) has(walName string) bool {
	_, ok := index.archives[walName]
	return ok
}

func newGreenplumCopyState(ctx context.Context, plan *copy.Plan) (*greenplumCopyState, error) {
	state := &greenplumCopyState{
		backupState:    make(map[string]uint8),
		backupDepths:   make(map[string]int),
		segmentState:   make(map[string]uint8),
		segmentDepths:  make(map[string]int),
		segmentBackups: make(map[string]postgres.Backup),
		walFolders:     make(map[string]walFolderIndex),
		filesMetadata:  make(map[string]struct{}),
	}
	walDir := strings.TrimSuffix(utility.WalPath, "/")
	basePrefix := strings.TrimSuffix(utility.BaseBackupPath, "/") + "/"
	restorePointPaths := make([]string, 0)
	for _, object := range plan.SourceObjects() {
		name := object.GetName()
		switch {
		case strings.HasPrefix(name, basePrefix) && !strings.Contains(strings.TrimPrefix(name, basePrefix), "/") &&
			strings.HasSuffix(name, RestorePointSuffix):
			restorePointPaths = append(restorePointPaths, name)
		case path.Base(name) == ao.FilesMetadataName || path.Base(name) == pax.FilesMetadataName:
			state.filesMetadata[name] = struct{}{}
		case path.Base(path.Dir(name)) == walDir:
			state.indexWAL(name)
		}
	}
	points, err := fetchRestorePoints(ctx, plan.From, restorePointPaths)
	if err != nil {
		return nil, err
	}
	state.restorePoints = points
	return state, nil
}

func (state *greenplumCopyState) indexWAL(name string) {
	folder := path.Dir(name)
	index, ok := state.walFolders[folder]
	if !ok {
		index = walFolderIndex{
			archives: make(map[string]struct{}),
			segments: make(map[postgres.WalSegmentNo][]string),
		}
		state.walFolders[folder] = index
	}
	archive := copy.StripCompressionExtension(path.Base(name))
	index.archives[archive] = struct{}{}
	if _, segmentNo, err := postgres.ParseWALFilename(archive); err == nil {
		walSegmentNo := postgres.WalSegmentNo(segmentNo)
		index.segments[walSegmentNo] = append(index.segments[walSegmentNo], name)
	}
}

func (state *greenplumCopyState) walFolder(contentID int) walFolderIndex {
	return state.walFolders[FormatSegmentWalPath(contentID)]
}

func segmentBackupKey(contentID int, name string) string {
	return fmt.Sprintf("%d/%s", contentID, name)
}

func fetchRestorePoints(
	ctx context.Context,
	folder storage.Folder,
	objectPaths []string,
) (map[string]RestorePointMetadata, error) {
	points := make([]RestorePointMetadata, len(objectPaths))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(restorePointFetchConcurrency)
	for i, objectPath := range objectPaths {
		group.Go(func() error {
			name := StripRightmostRestorePointName(objectPath)
			point, err := FetchRestorePointMetadata(groupCtx, folder, name)
			if err != nil {
				return err
			}
			point.Name = name
			points[i] = point
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	byName := make(map[string]RestorePointMetadata, len(points))
	for i := range points {
		byName[points[i].Name] = points[i]
	}
	return byName, nil
}

func BuildCopyPlan(
	ctx context.Context,
	from, to storage.Folder,
	backupName string,
	withHistory bool,
) (*copy.Plan, error) {
	plan, err := copy.NewPlan(ctx, from, to)
	if err != nil {
		return nil, err
	}
	names, err := plan.ResolveBackupNames(ctx, backupName)
	if err != nil {
		return nil, err
	}

	state, err := newGreenplumCopyState(ctx, plan)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := addGreenplumBackupToPlan(ctx, plan, name, withHistory, state); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func addGreenplumBackupToPlan(
	ctx context.Context,
	plan *copy.Plan,
	name string,
	withHistory bool,
	state *greenplumCopyState,
) error {
	backup, sentinel, depth, err := addGreenplumBackupChain(ctx, plan, name, state)
	if err != nil {
		return err
	}

	if sentinel.RestorePoint == nil {
		return fmt.Errorf("greenplum backup %q has no restore point", name)
	}
	selected, err := selectedGreenplumRestorePoint(ctx, plan, state, *sentinel.RestorePoint)
	if err != nil {
		return err
	}
	if !greenplumRestorePointMatchesBackup(&sentinel, selected) {
		return fmt.Errorf("selected Greenplum restore point %q does not match backup %q system or topology",
			selected.Name, name)
	}
	points := []RestorePointMetadata{selected}
	endpoint := selected
	if withHistory {
		if endpoint, err = latestGreenplumRestorePoint(state, &sentinel); err != nil {
			return err
		}
		if endpoint.Name != selected.Name {
			points = append(points, endpoint)
		}
	}
	for i := range points {
		if err := validateGreenplumRestorePointTimelines(points[i]); err != nil {
			return err
		}
	}
	lastWAL, err := addGreenplumSegmentHistory(ctx, plan, state, &sentinel, points)
	if err != nil {
		return err
	}
	if err := addGreenplumRestorePointMetadata(plan, state, &sentinel, selected, endpoint, lastWAL); err != nil {
		return err
	}

	topSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), internal.SentinelNameFromBackup(backup.Name))
	return plan.SetOrder(topSentinel, copy.FinalCommitPhase, depth)
}

func addGreenplumBackupChain(
	ctx context.Context,
	plan *copy.Plan,
	name string,
	state *greenplumCopyState,
) (internal.Backup, BackupSentinelDto, int, error) {
	if state.backupState[name] == 1 {
		return internal.Backup{}, BackupSentinelDto{}, 0, fmt.Errorf("cycle in Greenplum incremental backup chain at %q", name)
	}
	backup, err := internal.GetBackupByName(ctx, name, utility.BaseBackupPath, plan.From)
	if err != nil {
		return internal.Backup{}, BackupSentinelDto{}, 0, err
	}
	var sentinel BackupSentinelDto
	if err := backup.FetchSentinel(ctx, &sentinel); err != nil {
		return internal.Backup{}, BackupSentinelDto{}, 0, fmt.Errorf("read Greenplum backup %q sentinel: %w", name, err)
	}
	if state.backupState[name] == 2 {
		return backup, sentinel, state.backupDepths[name], nil
	}
	state.backupState[name] = 1
	depth := 0
	if sentinel.IncrementFrom != nil {
		_, _, parentDepth, err := addGreenplumBackupChain(ctx, plan, *sentinel.IncrementFrom, state)
		if err != nil {
			return internal.Backup{}, BackupSentinelDto{}, 0, err
		}
		depth = parentDepth + 1
	}
	for _, segment := range sentinel.Segments {
		if _, err := addGreenplumSegmentChain(ctx, plan, segment.ContentID, segment.BackupName, state); err != nil {
			return internal.Backup{}, BackupSentinelDto{}, 0, err
		}
	}
	topSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), internal.SentinelNameFromBackup(name))
	if err := plan.AddObject(topSentinel, topSentinel, copy.AggregateCommitPhase, false); err != nil {
		return internal.Backup{}, BackupSentinelDto{}, 0, err
	}
	if err := plan.SetOrder(topSentinel, copy.AggregateCommitPhase, depth); err != nil {
		return internal.Backup{}, BackupSentinelDto{}, 0, err
	}
	state.backupDepths[name] = depth
	state.backupState[name] = 2
	return backup, sentinel, depth, nil
}

func addGreenplumSegmentChain(
	ctx context.Context,
	plan *copy.Plan,
	contentID int,
	name string,
	state *greenplumCopyState,
) (int, error) {
	key := segmentBackupKey(contentID, name)
	switch state.segmentState[key] {
	case 1:
		return 0, fmt.Errorf("cycle in Greenplum segment %d backup chain at %q", contentID, name)
	case 2:
		return state.segmentDepths[key], nil
	}
	basePath := FormatSegmentBackupPath(contentID)
	backup, err := internal.GetBackupByName(ctx, name, basePath, plan.From)
	if err != nil {
		return 0, err
	}
	pgBackup := postgres.ToPgBackup(backup)
	sentinel, err := pgBackup.GetSentinel(ctx)
	if err != nil {
		return 0, fmt.Errorf("read Greenplum segment %d backup %q sentinel: %w", contentID, name, err)
	}
	state.segmentState[key] = 1
	depth := 0
	if sentinel.IncrementFrom != nil {
		parentDepth, err := addGreenplumSegmentChain(ctx, plan, contentID, *sentinel.IncrementFrom, state)
		if err != nil {
			return 0, err
		}
		depth = parentDepth + 1
	}
	if err := plan.AddBackupAt(basePath, name, name); err != nil {
		return 0, err
	}
	if err := addGreenplumSegmentSharedFiles(ctx, plan, state, basePath, backup); err != nil {
		return 0, fmt.Errorf("plan shared files for Greenplum segment %d backup %q: %w", contentID, name, err)
	}
	targetSentinel := path.Join(basePath, internal.SentinelNameFromBackup(name))
	if err := plan.SetOrder(targetSentinel, copy.BackupCommitPhase, depth); err != nil {
		return 0, err
	}
	state.segmentDepths[key] = depth
	state.segmentBackups[key] = pgBackup
	state.segmentState[key] = 2
	return depth, nil
}

func addGreenplumSegmentSharedFiles(
	ctx context.Context,
	plan *copy.Plan,
	state *greenplumCopyState,
	basePath string,
	backup internal.Backup,
) error {
	baseBackupsFolder := plan.From.GetSubFolder(basePath)
	backupTime := internal.BackupTime{BackupName: backup.Name, StorageName: backup.GetStorageName()}
	sharedStorages := []struct {
		storagePath  string
		metadataPath func(backupName string) string
		fetch        referencedFilesFetcher
	}{
		{storagePath: ao.StoragePath, metadataPath: ao.GetFilesMetadataPath, fetch: ao.FetchReferencedFiles},
		{storagePath: pax.StoragePath, metadataPath: pax.GetFilesMetadataPath, fetch: pax.FetchReferencedFiles},
	}
	for _, shared := range sharedStorages {
		if _, ok := state.filesMetadata[path.Join(basePath, shared.metadataPath(backup.Name))]; !ok {
			continue
		}
		files, err := shared.fetch(ctx, baseBackupsFolder, backupTime)
		if err != nil {
			return fmt.Errorf("read %s files metadata: %w", shared.storagePath, err)
		}
		for storagePath := range files {
			name := path.Join(basePath, shared.storagePath, storagePath)
			if err := plan.AddObject(name, name, copy.PayloadPhase, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func selectedGreenplumRestorePoint(
	ctx context.Context,
	plan *copy.Plan,
	state *greenplumCopyState,
	name string,
) (RestorePointMetadata, error) {
	if point, ok := state.restorePoints[name]; ok {
		return point, nil
	}
	if _, err := FetchRestorePointMetadata(ctx, plan.From, name); err != nil {
		return RestorePointMetadata{}, err
	}
	return RestorePointMetadata{}, notPublishedError{object: fmt.Sprintf("restore point %q metadata", name)}
}

func latestGreenplumRestorePoint(state *greenplumCopyState, backup *BackupSentinelDto) (RestorePointMetadata, error) {
	var latest *RestorePointMetadata
	for name := range state.restorePoints {
		point := state.restorePoints[name]
		if point.FinishTime.Before(backup.FinishTime) || !greenplumRestorePointMatchesBackup(backup, point) {
			continue
		}
		if latest == nil || point.FinishTime.After(latest.FinishTime) ||
			(point.FinishTime.Equal(latest.FinishTime) && point.Name > latest.Name) {
			latest = &point
		}
	}
	if latest == nil {
		return RestorePointMetadata{}, fmt.Errorf(
			"no compatible Greenplum restore point exists at or after the selected backup")
	}
	return *latest, nil
}

func greenplumRestorePointMatchesBackup(backup *BackupSentinelDto, point RestorePointMetadata) bool {
	if backup.SystemIdentifier != nil && point.SystemIdentifier != nil &&
		*backup.SystemIdentifier != *point.SystemIdentifier {
		return false
	}
	if len(point.LsnBySegment) != len(backup.Segments) {
		return false
	}
	for _, segment := range backup.Segments {
		if _, ok := point.LsnBySegment[segment.ContentID]; !ok {
			return false
		}
	}
	return true
}

func validateGreenplumRestorePointTimelines(point RestorePointMetadata) error {
	if len(point.TimelineBySegment) == 0 {
		return nil
	}
	if err := validateRestorePointTimelines(point.LsnBySegment, point.TimelineBySegment); err != nil {
		return fmt.Errorf("validate Greenplum restore point %q: %w", point.Name, err)
	}
	return nil
}

func addGreenplumSegmentHistory(
	ctx context.Context,
	plan *copy.Plan,
	state *greenplumCopyState,
	backup *BackupSentinelDto,
	points []RestorePointMetadata,
) (map[int]string, error) {
	lastWAL := make(map[int]string, len(backup.Segments))
	for _, segment := range backup.Segments {
		pgBackup, ok := state.segmentBackups[segmentBackupKey(segment.ContentID, segment.BackupName)]
		if !ok {
			return nil, fmt.Errorf("greenplum segment %d backup %q is not planned", segment.ContentID, segment.BackupName)
		}
		wal := state.walFolder(segment.ContentID)
		last, err := greenplumStopWAL(ctx, pgBackup)
		if err != nil {
			return nil, fmt.Errorf("find stop WAL of Greenplum segment %d backup %q: %w",
				segment.ContentID, segment.BackupName, err)
		}
		if !wal.has(last) {
			return nil, fmt.Errorf("stop WAL %q of Greenplum segment %d backup %q is missing",
				last, segment.ContentID, segment.BackupName)
		}
		for i := range points {
			name, err := greenplumRestorePointWAL(points[i], segment.ContentID, wal)
			if err != nil {
				return nil, err
			}
			last = max(last, name)
		}
		segmentRoot := FormatSegmentStoragePrefix(segment.ContentID)
		if err := postgres.AddHistoryToPlan(ctx, plan, pgBackup, segmentRoot, false, last); err != nil {
			return nil, fmt.Errorf("plan WAL for Greenplum segment %d: %w", segment.ContentID, err)
		}
		lastWAL[segment.ContentID] = last
	}
	return lastWAL, nil
}

func greenplumStopWAL(ctx context.Context, backup postgres.Backup) (string, error) {
	meta, err := backup.FetchMeta(ctx)
	if err != nil {
		return "", err
	}
	timeline, err := postgres.ParseTimelineFromBackupName(backup.Name)
	if err != nil {
		return "", err
	}
	return walSegmentNoOfRecordEnd(meta.FinishLsn).GetFilename(timeline), nil
}

func greenplumRestorePointWAL(point RestorePointMetadata, contentID int, wal walFolderIndex) (string, error) {
	lsnText, ok := point.LsnBySegment[contentID]
	if !ok {
		return "", fmt.Errorf("greenplum restore point %q has no LSN for segment %d", point.Name, contentID)
	}
	lsn, err := postgres.ParseLSN(lsnText)
	if err != nil {
		return "", fmt.Errorf("parse restore point %q LSN for Greenplum segment %d: %w", point.Name, contentID, err)
	}
	walSegmentNo := walSegmentNoOfRecordEnd(lsn)
	candidates := wal.segments[walSegmentNo]
	if len(candidates) == 0 {
		return "", notPublishedError{object: fmt.Sprintf(
			"WAL segment %d of restore point %q on Greenplum segment %d", walSegmentNo, point.Name, contentID)}
	}
	timeline, err := resolveGreenplumTimeline(point, contentID, walSegmentNo, candidates)
	if err != nil {
		return "", err
	}
	name := walSegmentNo.GetFilename(timeline)
	if !wal.has(name) {
		return "", notPublishedError{object: fmt.Sprintf(
			"WAL %s of restore point %q on Greenplum segment %d", name, point.Name, contentID)}
	}
	return name, nil
}

func walSegmentNoOfRecordEnd(lsn postgres.LSN) postgres.WalSegmentNo {
	if lsn == 0 {
		return postgres.NewWalSegmentNo(lsn)
	}
	return postgres.NewWalSegmentNo(lsn - 1)
}

func resolveGreenplumTimeline(
	metadata RestorePointMetadata,
	contentID int,
	walSegmentNo postgres.WalSegmentNo,
	walObjects []string,
) (uint32, error) {
	if len(metadata.TimelineBySegment) > 0 {
		timeline, ok := metadata.TimelineBySegment[contentID]
		if ok && timeline != 0 {
			return timeline, nil
		}
		if contentID != -1 {
			return 0, fmt.Errorf("greenplum restore point %q has no timeline for segment %d", metadata.Name, contentID)
		}
	}
	if contentID == -1 && metadata.TimeLine != 0 {
		return metadata.TimeLine, nil
	}
	if len(metadata.TimelineBySegment) > 0 {
		return 0, fmt.Errorf("greenplum restore point %q has no timeline for segment %d", metadata.Name, contentID)
	}

	candidates := make(map[uint32]struct{})
	for _, name := range walObjects {
		archiveName := copy.StripCompressionExtension(path.Base(name))
		timeline, segmentNo, err := postgres.ParseWALFilename(archiveName)
		if err == nil && postgres.WalSegmentNo(segmentNo) == walSegmentNo {
			candidates[timeline] = struct{}{}
		}
	}
	if len(candidates) != 1 {
		return 0, fmt.Errorf(
			"cannot safely infer timeline for legacy Greenplum restore point %q segment %d: found %d endpoint WAL timelines",
			metadata.Name, contentID, len(candidates))
	}
	for timeline := range candidates {
		return timeline, nil
	}
	return 0, fmt.Errorf(
		"cannot infer timeline for legacy Greenplum restore point %q segment %d",
		metadata.Name, contentID)
}

func addGreenplumRestorePointMetadata(
	plan *copy.Plan,
	state *greenplumCopyState,
	backup *BackupSentinelDto,
	selected, endpoint RestorePointMetadata,
	lastWAL map[int]string,
) error {
	for name := range state.restorePoints {
		point := state.restorePoints[name]
		if name != selected.Name && name != endpoint.Name {
			if point.FinishTime.Before(selected.FinishTime) || point.FinishTime.After(endpoint.FinishTime) ||
				!greenplumRestorePointMatchesBackup(backup, point) {
				continue
			}
			if err := checkGreenplumRestorePointWAL(point, state, lastWAL); err != nil {
				tracelog.WarningLogger.Printf("Not copying Greenplum restore point %q: %v", name, err)
				continue
			}
		}
		metadataPath := path.Join(utility.BaseBackupPath, RestorePointMetadataFileName(name))
		if err := plan.AddObject(metadataPath, metadataPath, copy.RecoveryMetadataPhase, false); err != nil {
			return err
		}
	}
	return nil
}

func checkGreenplumRestorePointWAL(point RestorePointMetadata, state *greenplumCopyState, lastWAL map[int]string) error {
	if err := validateGreenplumRestorePointTimelines(point); err != nil {
		return err
	}
	for contentID := range point.LsnBySegment {
		name, err := greenplumRestorePointWAL(point, contentID, state.walFolder(contentID))
		if err != nil {
			return err
		}
		if name > lastWAL[contentID] {
			return fmt.Errorf("its WAL %s of segment %d lies past the copied WAL", name, contentID)
		}
	}
	return nil
}
