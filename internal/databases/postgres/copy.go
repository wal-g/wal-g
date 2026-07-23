package postgres

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(ctx context.Context, fromConfigFile string, toConfigFile string, backupName string, withAllHistory bool) {
	from, err := internal.StorageFromConfig(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, err := internal.StorageFromConfig(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := BuildCopyPlan(ctx, from.RootFolder(), to.RootFolder(), backupName, withAllHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copy.ExecuteRaw(ctx, plan))
	tracelog.InfoLogger.Println("Success copy.")
}

// BuildCopyPlan builds the minimum restorable backup closure and, when
// requested, extends its WAL range through the latest continuous archive.
func BuildCopyPlan(
	ctx context.Context,
	from, to storage.Folder,
	backupName string,
	withAllHistory bool,
) (*copy.Plan, error) {
	plan, err := copy.NewPlan(ctx, from, to)
	if err != nil {
		return nil, err
	}
	names, err := plan.ResolveBackupNames(ctx, backupName)
	if err != nil {
		return nil, err
	}

	state := make(map[string]uint8)
	depths := make(map[string]int)
	for _, name := range names {
		backup, depth, err := addPostgresBackupChain(ctx, plan, name, state, depths)
		if err != nil {
			return nil, err
		}
		targetSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), internal.SentinelNameFromBackup(name))
		if err := plan.SetPhase(targetSentinel, copy.CommitPhase+copy.Phase(depth)); err != nil {
			return nil, err
		}
		if err := addPostgresHistory(ctx, plan, backup, withAllHistory); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func addPostgresBackupChain(
	ctx context.Context,
	plan *copy.Plan,
	name string,
	state map[string]uint8,
	depths map[string]int,
) (Backup, int, error) {
	if state[name] == 1 {
		return Backup{}, 0, fmt.Errorf("cycle in PostgreSQL incremental backup chain at %q", name)
	}
	backup, err := internal.GetBackupByName(ctx, name, utility.BaseBackupPath, plan.From)
	if err != nil {
		return Backup{}, 0, err
	}
	pgBackup := ToPgBackup(backup)
	sentinel, err := pgBackup.GetSentinel(ctx)
	if err != nil {
		return Backup{}, 0, fmt.Errorf("read PostgreSQL backup %q sentinel: %w", name, err)
	}
	if state[name] == 2 {
		return pgBackup, depths[name], nil
	}
	state[name] = 1
	depth := 0
	if sentinel.IncrementFrom != nil {
		_, parentDepth, err := addPostgresBackupChain(ctx, plan, *sentinel.IncrementFrom, state, depths)
		if err != nil {
			return Backup{}, 0, err
		}
		depth = parentDepth + 1
	}
	if err := plan.AddBackup(name, name); err != nil {
		return Backup{}, 0, err
	}
	targetSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"), internal.SentinelNameFromBackup(name))
	if err := plan.SetPhase(targetSentinel, copy.CommitPhase+copy.Phase(depth)); err != nil {
		return Backup{}, 0, err
	}
	depths[name] = depth
	state[name] = 2
	return pgBackup, depth, nil
}

func addPostgresHistory(ctx context.Context, plan *copy.Plan, backup Backup, withAllHistory bool) error {
	return AddHistoryToPlan(ctx, plan, backup, "", withAllHistory, "")
}

type postgresHistoryScan struct {
	segments   map[uint32]map[uint64]bool
	foundFirst bool
	foundLast  bool
}

// AddHistoryToPlan adds a PostgreSQL-compatible WAL stream rooted at prefix.
// explicitLast is used by Greenplum cluster restore points; an empty value uses
// the backup finish LSN, while withAllHistory leaves the upper bound open.
func AddHistoryToPlan(
	ctx context.Context,
	plan *copy.Plan,
	backup Backup,
	prefix string,
	withAllHistory bool,
	explicitLast string,
) error {
	first, err := GetFirstWalFilename(ctx, backup)
	if err != nil {
		return err
	}
	last := explicitLast
	if last == "" {
		last, err = GetLastWalFilename(ctx, backup)
		if err != nil {
			return err
		}
	}

	walPrefix := strings.TrimSuffix(path.Join(prefix, utility.WalPath), "/") + "/"
	history, err := addPostgresHistoryObjects(plan, walPrefix, first, last, withAllHistory)
	if err != nil {
		return err
	}
	return validatePostgresHistory(history, first, last, withAllHistory)
}

func addPostgresHistoryObjects(
	plan *copy.Plan,
	walPrefix string,
	first string,
	last string,
	withAllHistory bool,
) (postgresHistoryScan, error) {
	history := postgresHistoryScan{segments: make(map[uint32]map[uint64]bool)}
	for _, object := range plan.SourceObjects() {
		name := object.GetName()
		if !strings.HasPrefix(name, walPrefix) {
			continue
		}
		baseName := path.Base(name)
		archiveName := copy.StripCompressionExtension(baseName)
		logicalName := GetWalFileName(archiveName)
		isTimelineHistory := strings.HasSuffix(archiveName, ".history")
		// Timeline files are small, opaque restore dependencies. They bypass
		// range filtering to avoid opening or decrypting them to infer ancestry.
		if !isTimelineHistory && (logicalName < first || (!withAllHistory && logicalName > last)) {
			continue
		}
		if err := plan.AddObject(name, name, copy.PayloadPhase, false); err != nil {
			return postgresHistoryScan{}, err
		}
		// Sidecars share the complete segment's prefix, but cannot be fetched as
		// that segment. Only an exact WAL archive may prove continuity.
		timeline, segmentNo, parseErr := ParseWALFilename(archiveName)
		if parseErr != nil {
			continue
		}
		if history.segments[timeline] == nil {
			history.segments[timeline] = make(map[uint64]bool)
		}
		history.segments[timeline][segmentNo] = true
		if archiveName == first {
			history.foundFirst = true
		}
		if archiveName == last {
			history.foundLast = true
		}
	}
	return history, nil
}

func validatePostgresHistory(history postgresHistoryScan, first, last string, withAllHistory bool) error {
	if !history.foundFirst {
		return fmt.Errorf("first required PostgreSQL WAL segment %q is missing", first)
	}
	if !withAllHistory && !history.foundLast {
		return fmt.Errorf("last required PostgreSQL WAL segment %q is missing", last)
	}
	for timeline, numbers := range history.segments {
		ordered := make([]uint64, 0, len(numbers))
		for number := range numbers {
			ordered = append(ordered, number)
		}
		slices.Sort(ordered)
		for i := 1; i < len(ordered); i++ {
			if ordered[i] != ordered[i-1]+1 {
				return fmt.Errorf("gap in PostgreSQL WAL history on timeline %08X between segments %d and %d",
					timeline, ordered[i-1], ordered[i])
			}
		}
	}
	return nil
}

func BackupCopyingInfo(ctx context.Context, backup Backup, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)
	backupDataPrefix := backupPrefix + "/"
	backupSentinel := backupPrefix + utility.SentinelSuffix

	var objects, err = storage.ListFolderRecursively(ctx, from)
	if err != nil {
		return nil, err
	}

	var hasBackupPrefix = func(object storage.Object) bool {
		return strings.HasPrefix(object.GetName(), backupDataPrefix) || object.GetName() == backupSentinel
	}
	return copy.BuildCopyingInfos(
		from,
		to,
		objects,
		hasBackupPrefix,
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	), nil
}

func HistoryCopyingInfo(ctx context.Context, backup Backup, from storage.Folder, to storage.Folder, withAllHistory bool,
) ([]copy.InfoProvider, error) {
	tracelog.DebugLogger.Print("Collecting history files... ")

	var fromWalFolder = from.GetSubFolder(utility.WalPath)

	var lastWalFilename, err = GetLastWalFilename(ctx, backup)
	if err != nil {
		return make([]copy.InfoProvider, 0), err
	}

	firstWalFilename, err := GetFirstWalFilename(ctx, backup)
	if err != nil {
		return make([]copy.InfoProvider, 0), err
	}

	tracelog.DebugLogger.Print("getLastWalFilename not failed!")

	objects, err := storage.ListFolderRecursively(ctx, fromWalFolder)
	if err != nil {
		return nil, err
	}

	var match = func(object storage.Object) bool {
		return GetWalFileName(object.GetName()) >= firstWalFilename &&
			(withAllHistory || GetWalFileName(object.GetName()) <= lastWalFilename)
	}
	return copy.BuildCopyingInfos(
		fromWalFolder,
		to.GetSubFolder(utility.WalPath),
		objects,
		match,
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	), nil
}

func GetWalFileName(filename string) string {
	if !strings.Contains(filename, ".") {
		return filename
	}
	return strings.Split(filename, ".")[0]
}

func WildcardInfo(ctx context.Context, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	objects, err := storage.ListFolderRecursively(ctx, from)
	if err != nil {
		return nil, err
	}

	return copy.BuildCopyingInfos(
		from,
		to,
		objects,
		func(object storage.Object) bool { return true },
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	), nil
}
