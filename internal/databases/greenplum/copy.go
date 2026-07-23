package greenplum

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopy preserves the original exact-restore copy API.
func HandleCopy(ctx context.Context, fromConfigFile string, toConfigFile string, backupName string) {
	HandleCopyWithHistory(ctx, fromConfigFile, toConfigFile, backupName, false)
}

// HandleCopyWithHistory copies specific or all backups and optionally extends
// each segment WAL stream through the latest cluster restore point.
func HandleCopyWithHistory(ctx context.Context, fromConfigFile string, toConfigFile string, backupName string, withHistory bool) {
	from, err := internal.StorageFromConfig(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, err := internal.StorageFromConfig(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := BuildCopyPlan(ctx, from.RootFolder(), to.RootFolder(), backupName, withHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copy.ExecuteRaw(ctx, plan))
	tracelog.InfoLogger.Println("Success copy.")
}

type greenplumCopyState struct {
	backupState   map[string]uint8
	backupDepths  map[string]int
	segmentState  map[string]uint8
	segmentDepths map[string]int
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

	state := &greenplumCopyState{
		backupState:   make(map[string]uint8),
		backupDepths:  make(map[string]int),
		segmentState:  make(map[string]uint8),
		segmentDepths: make(map[string]int),
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
	selectedPoint, err := FetchRestorePointMetadata(ctx, plan.From, *sentinel.RestorePoint)
	if err != nil {
		return err
	}
	if !greenplumRestorePointMatchesBackup(&sentinel, selectedPoint) {
		return fmt.Errorf("selected Greenplum restore point %q does not match backup %q system or topology",
			selectedPoint.Name, name)
	}
	if len(selectedPoint.TimelineBySegment) > 0 {
		if err := validateRestorePointTimelines(selectedPoint.LsnBySegment, selectedPoint.TimelineBySegment); err != nil {
			return fmt.Errorf("validate selected Greenplum restore point %q: %w", selectedPoint.Name, err)
		}
	}
	endpoint := &selectedPoint
	if withHistory {
		value, err := latestGreenplumRestorePoint(ctx, plan, &sentinel)
		if err != nil {
			return err
		}
		endpoint = &value
	}
	if len(endpoint.TimelineBySegment) > 0 {
		if err := validateRestorePointTimelines(endpoint.LsnBySegment, endpoint.TimelineBySegment); err != nil {
			return fmt.Errorf("validate endpoint Greenplum restore point %q: %w", endpoint.Name, err)
		}
	}
	if err := addGreenplumSegmentHistory(ctx, plan, &sentinel, endpoint); err != nil {
		return err
	}
	if err := addGreenplumRestorePointMetadata(ctx, plan, &sentinel, endpoint); err != nil {
		return err
	}

	topSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), internal.SentinelNameFromBackup(backup.Name))
	return plan.SetPhase(topSentinel, copy.CommitPhase+200+copy.Phase(depth))
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
		if _, err := addGreenplumSegmentChain(
			ctx, plan, segment.ContentID, segment.BackupName, state.segmentState, state.segmentDepths); err != nil {
			return internal.Backup{}, BackupSentinelDto{}, 0, err
		}
	}
	if err := plan.AddBackup(name, name); err != nil {
		return internal.Backup{}, BackupSentinelDto{}, 0, err
	}
	topSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), internal.SentinelNameFromBackup(name))
	if err := plan.SetPhase(topSentinel, copy.CommitPhase+100+copy.Phase(depth)); err != nil {
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
	state map[string]uint8,
	depths map[string]int,
) (int, error) {
	key := fmt.Sprintf("%d/%s", contentID, name)
	if state[key] == 1 {
		return 0, fmt.Errorf("cycle in Greenplum segment %d backup chain at %q", contentID, name)
	}
	segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, contentID)
	basePath := path.Join(segmentRoot, utility.BaseBackupPath)
	backup, err := internal.GetBackupByName(ctx, name, basePath, plan.From)
	if err != nil {
		return 0, err
	}
	pgBackup := postgres.ToPgBackup(backup)
	sentinel, err := pgBackup.GetSentinel(ctx)
	if err != nil {
		return 0, fmt.Errorf("read Greenplum segment %d backup %q sentinel: %w", contentID, name, err)
	}
	if state[key] == 2 {
		return depths[key], nil
	}
	state[key] = 1
	depth := 0
	if sentinel.IncrementFrom != nil {
		parentDepth, err := addGreenplumSegmentChain(ctx, plan, contentID, *sentinel.IncrementFrom, state, depths)
		if err != nil {
			return 0, err
		}
		depth = parentDepth + 1
	}
	if err := plan.AddBackupAt(basePath, name, name); err != nil {
		return 0, err
	}
	targetSentinel := path.Join(strings.TrimSuffix(basePath, "/"), internal.SentinelNameFromBackup(name))
	if err := plan.SetPhase(targetSentinel, copy.CommitPhase+copy.Phase(depth)); err != nil {
		return 0, err
	}
	depths[key] = depth
	state[key] = 2
	return depth, nil
}

func latestGreenplumRestorePoint(
	ctx context.Context,
	plan *copy.Plan,
	backup *BackupSentinelDto,
) (RestorePointMetadata, error) {
	basePrefix := strings.TrimSuffix(utility.BaseBackupPath, "/") + "/"
	points := make([]RestorePointMetadata, 0)
	for _, object := range plan.SourceObjects() {
		name := object.GetName()
		if !strings.HasPrefix(name, basePrefix) || strings.Contains(strings.TrimPrefix(name, basePrefix), "/") ||
			!strings.HasSuffix(name, RestorePointSuffix) {
			continue
		}
		pointName := strings.TrimSuffix(path.Base(name), RestorePointSuffix)
		point, err := FetchRestorePointMetadata(ctx, plan.From, pointName)
		if err != nil {
			return RestorePointMetadata{}, err
		}
		if !point.FinishTime.Before(backup.FinishTime) && greenplumRestorePointMatchesBackup(backup, point) {
			points = append(points, point)
		}
	}
	if len(points) == 0 {
		return RestorePointMetadata{}, fmt.Errorf(
			"no compatible Greenplum restore point exists at or after the selected backup")
	}
	slices.SortFunc(points, func(a, b RestorePointMetadata) int { return a.FinishTime.Compare(b.FinishTime) })
	return points[len(points)-1], nil
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

func addGreenplumSegmentHistory(
	ctx context.Context,
	plan *copy.Plan,
	backup *BackupSentinelDto,
	endpoint *RestorePointMetadata,
) error {
	for _, segment := range backup.Segments {
		segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, segment.ContentID)
		basePath := path.Join(segmentRoot, utility.BaseBackupPath)
		internalBackup, err := internal.GetBackupByName(ctx, segment.BackupName, basePath, plan.From)
		if err != nil {
			return err
		}
		pgBackup := postgres.ToPgBackup(internalBackup)
		explicitLast := ""
		if endpoint != nil {
			lsn, err := postgres.ParseLSN(endpoint.LsnBySegment[segment.ContentID])
			if err != nil {
				return fmt.Errorf("parse restore point LSN for Greenplum segment %d: %w", segment.ContentID, err)
			}
			walSegmentNo := postgres.NewWalSegmentNo(lsn)
			walPrefix := strings.TrimSuffix(path.Join(segmentRoot, utility.WalPath), "/") + "/"
			walObjects := make([]string, 0)
			for _, object := range plan.SourceObjects() {
				if strings.HasPrefix(object.GetName(), walPrefix) {
					walObjects = append(walObjects, object.GetName())
				}
			}
			timeline, err := resolveGreenplumTimeline(*endpoint, segment.ContentID, walSegmentNo, walObjects)
			if err != nil {
				return err
			}
			explicitLast = walSegmentNo.GetFilename(timeline)
		}
		if err := postgres.AddHistoryToPlan(ctx, plan, pgBackup, segmentRoot, false, explicitLast); err != nil {
			return fmt.Errorf("plan WAL for Greenplum segment %d: %w", segment.ContentID, err)
		}
	}
	return nil
}

func resolveGreenplumTimeline(
	metadata RestorePointMetadata,
	contentID int,
	walSegmentNo postgres.WalSegmentNo,
	walObjects []string,
) (uint32, error) {
	if len(metadata.TimelineBySegment) > 0 {
		timeline, ok := metadata.TimelineBySegment[contentID]
		if !ok || timeline == 0 {
			return 0, fmt.Errorf("greenplum restore point %q has no timeline for segment %d", metadata.Name, contentID)
		}
		return timeline, nil
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
	panic("unreachable")
}

func addGreenplumRestorePointMetadata(
	ctx context.Context,
	plan *copy.Plan,
	backup *BackupSentinelDto,
	endpoint *RestorePointMetadata,
) error {
	if backup.RestorePoint == nil {
		return fmt.Errorf("greenplum backup has no restore point")
	}
	selected, err := FetchRestorePointMetadata(ctx, plan.From, *backup.RestorePoint)
	if err != nil {
		return err
	}
	upper := selected.FinishTime
	if endpoint != nil {
		upper = endpoint.FinishTime
	}
	basePrefix := strings.TrimSuffix(utility.BaseBackupPath, "/") + "/"
	for _, object := range plan.SourceObjects() {
		name := object.GetName()
		relative := strings.TrimPrefix(name, basePrefix)
		if !strings.HasPrefix(name, basePrefix) || strings.Contains(relative, "/") || !strings.HasSuffix(name, RestorePointSuffix) {
			continue
		}
		pointName := strings.TrimSuffix(path.Base(name), RestorePointSuffix)
		point, err := FetchRestorePointMetadata(ctx, plan.From, pointName)
		if err != nil {
			return err
		}
		if point.FinishTime.Before(selected.FinishTime) || point.FinishTime.After(upper) {
			continue
		}
		if !greenplumRestorePointMatchesBackup(backup, point) {
			continue
		}
		if len(point.TimelineBySegment) > 0 {
			if err := validateRestorePointTimelines(point.LsnBySegment, point.TimelineBySegment); err != nil {
				return fmt.Errorf("validate Greenplum restore point %q: %w", point.Name, err)
			}
		}
		if err := plan.AddObject(name, name, copy.CommitPhase+50, false); err != nil {
			return err
		}
	}
	return nil
}

func GetCopyingInfos(ctx context.Context, backupName string,
	from storage.Folder,
	to storage.Folder) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := internal.GetBackupByName(ctx, backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	pgBackup := postgres.ToPgBackup(backup)
	backupInfo, err := postgres.BackupCopyingInfo(ctx, pgBackup, from, to)
	if err != nil {
		return nil, err
	}
	infos := []copy.InfoProvider{}
	infos = append(infos, backupInfo...)

	var sentinel BackupSentinelDto
	err = backup.FetchSentinel(ctx, &sentinel)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to get backup %s", backupName)
		return nil, err
	}

	// Older callers of GetCopyingInfos expect the cluster restore-point
	// metadata alongside the top-level backup. Add it explicitly instead of
	// relying on a broad backup-name prefix match.
	restorePointName := backupName
	if sentinel.RestorePoint != nil {
		restorePointName = *sentinel.RestorePoint
	}
	restorePointPath := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), RestorePointMetadataFileName(restorePointName))
	objects, err := storage.ListFolderRecursively(ctx, from)
	if err != nil {
		return nil, err
	}
	infos = append(infos, copy.BuildCopyingInfos(
		from,
		to,
		objects,
		func(object storage.Object) bool { return object.GetName() == restorePointPath },
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	)...)

	for _, meta := range sentinel.Segments {
		fromSubfolder := from.GetSubFolder(fmt.Sprintf("%s/seg%d/", utility.SegmentsPath, meta.ContentID))
		toSubfolder := to.GetSubFolder(fmt.Sprintf("%s/seg%d/", utility.SegmentsPath, meta.ContentID))
		backup, err := internal.GetBackupByName(ctx, meta.BackupName,
			fmt.Sprintf("%s/seg%d/%s", utility.SegmentsPath, meta.ContentID, utility.BaseBackupPath), from)
		if err != nil {
			return nil, err
		}
		pgBackup := postgres.ToPgBackup(backup)

		backupInfo, err := postgres.BackupCopyingInfo(ctx, pgBackup, fromSubfolder, toSubfolder)
		if err != nil {
			return nil, err
		}
		infos = append(infos, backupInfo...)
		historyInfo, err := postgres.HistoryCopyingInfo(ctx, pgBackup, fromSubfolder, toSubfolder, false)
		if err != nil {
			return nil, err
		}

		infos = append(infos, historyInfo...)
	}

	return infos, nil
}
