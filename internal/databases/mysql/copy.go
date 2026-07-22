package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopyBackup copy specific backups from one storage to another
func HandleCopyBackup(ctx context.Context, fromConfigFile, toConfigFile, backupName, prefix string) {
	from, err := internal.StorageFromConfig(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, err := internal.StorageFromConfig(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := BuildCopyPlan(ctx, from.RootFolder(), to.RootFolder(), backupName, false, prefix)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copy.ExecuteRaw(ctx, plan))
	tracelog.InfoLogger.Printf("Successfully copied backup %s.\n", backupName)
}

// HandleCopyBackup copy  all backups from one storage to another
func HandleCopyAll(ctx context.Context, fromConfigFile string, toConfigFile string) {
	HandleCopy(ctx, fromConfigFile, toConfigFile, "", false)
	tracelog.InfoLogger.Printf("Successfully copied all backups\n")
}

func HandleCopy(ctx context.Context, fromConfigFile, toConfigFile, backupName string, withHistory bool) {
	from, err := internal.StorageFromConfig(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, err := internal.StorageFromConfig(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := BuildCopyPlan(ctx, from.RootFolder(), to.RootFolder(), backupName, withHistory, "")
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copy.ExecuteRaw(ctx, plan))
}

func BuildCopyPlan(
	ctx context.Context,
	from, to storage.Folder,
	backupName string,
	withHistory bool,
	prefix string,
) (*copy.Plan, error) {
	var binlogSentinelSnapshot []byte
	var hasBinlogSentinel bool
	if withHistory {
		var err error
		binlogSentinelSnapshot, hasBinlogSentinel, err = snapshotMySQLBinlogSentinel(ctx, from)
		if err != nil {
			return nil, err
		}
	}

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
	historyStarts := make(map[string]string)
	for _, name := range names {
		sentinel, depth, err := addMySQLBackupChain(ctx, plan, name, prefix, state, depths)
		if err != nil {
			return nil, err
		}
		targetSentinel := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"),
			internal.SentinelNameFromBackup(prefix+name))
		if err := plan.SetPhase(targetSentinel, copy.CommitPhase+copy.Phase(depth)); err != nil {
			return nil, err
		}
		if withHistory && sentinel.BinLogStart != "" {
			binlogPrefix := BinlogPrefix(sentinel.BinLogStart)
			if current, ok := historyStarts[binlogPrefix]; !ok || BinlogNum(sentinel.BinLogStart) < BinlogNum(current) {
				historyStarts[binlogPrefix] = sentinel.BinLogStart
			}
		}
	}

	if withHistory {
		if err := addMySQLHistoriesToPlan(plan, historyStarts, binlogSentinelSnapshot, hasBinlogSentinel); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func addMySQLHistoriesToPlan(
	plan *copy.Plan,
	historyStarts map[string]string,
	binlogSentinelSnapshot []byte,
	hasBinlogSentinel bool,
) error {
	if len(historyStarts) == 0 {
		return fmt.Errorf("selected MySQL backups contain no binlog recovery point")
	}
	for _, start := range historyStarts {
		if err := addMySQLHistory(plan, start); err != nil {
			return err
		}
	}
	if hasBinlogSentinel {
		plan.AddInline(BinlogSentinelPath, binlogSentinelSnapshot, copy.CommitPhase+100, true)
	}
	return nil
}

func snapshotMySQLBinlogSentinel(ctx context.Context, folder storage.Folder) ([]byte, bool, error) {
	reader, err := folder.ReadObject(ctx, BinlogSentinelPath)
	if err != nil {
		var notFound storage.ObjectNotFoundError
		if errors.As(err, &notFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read MySQL binlog sentinel: %w", err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, false, fmt.Errorf("read MySQL binlog sentinel: %w", readErr)
	}
	if closeErr != nil {
		return nil, false, fmt.Errorf("close MySQL binlog sentinel: %w", closeErr)
	}
	return data, true, nil
}

func addMySQLBackupChain(
	ctx context.Context,
	plan *copy.Plan,
	name, prefix string,
	state map[string]uint8,
	depths map[string]int,
) (StreamSentinelDto, int, error) {
	if state[name] == 1 {
		return StreamSentinelDto{}, 0, fmt.Errorf("cycle in MySQL incremental backup chain at %q", name)
	}
	backup, err := internal.GetBackupByName(ctx, name, utility.BaseBackupPath, plan.From)
	if err != nil {
		return StreamSentinelDto{}, 0, err
	}
	var sentinel StreamSentinelDto
	if err := backup.FetchSentinel(ctx, &sentinel); err != nil {
		return StreamSentinelDto{}, 0, fmt.Errorf("read MySQL backup %q sentinel: %w", name, err)
	}
	if state[name] == 2 {
		return sentinel, depths[name], nil
	}
	state[name] = 1
	depth := 0
	if sentinel.IncrementFrom != nil {
		_, parentDepth, err := addMySQLBackupChain(ctx, plan, *sentinel.IncrementFrom, prefix, state, depths)
		if err != nil {
			return StreamSentinelDto{}, 0, err
		}
		depth = parentDepth + 1
	}

	if err := plan.AddBackup(name, prefix+name); err != nil {
		return StreamSentinelDto{}, 0, err
	}
	if prefix != "" {
		rewritten := sentinel
		if rewritten.IncrementFrom != nil {
			value := prefix + *rewritten.IncrementFrom
			rewritten.IncrementFrom = &value
		}
		if rewritten.IncrementFullName != nil {
			value := prefix + *rewritten.IncrementFullName
			rewritten.IncrementFullName = &value
		}
		data, err := json.Marshal(&rewritten)
		if err != nil {
			return StreamSentinelDto{}, 0, fmt.Errorf("rewrite MySQL backup %q sentinel: %w", name, err)
		}
		target := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"),
			internal.SentinelNameFromBackup(prefix+name))
		plan.AddInline(target, data, copy.CommitPhase+copy.Phase(depth), false)
	}
	target := path.Join(strings.TrimSuffix(utility.BaseBackupPath, "/"),
		internal.SentinelNameFromBackup(prefix+name))
	if err := plan.SetPhase(target, copy.CommitPhase+copy.Phase(depth)); err != nil {
		return StreamSentinelDto{}, 0, err
	}
	depths[name] = depth
	state[name] = 2
	return sentinel, depth, nil
}

func addMySQLHistory(plan *copy.Plan, start string) error {
	type binlogObject struct {
		path string
		name string
		num  int
	}
	prefix := BinlogPrefix(start)
	startNum := BinlogNum(start)
	objects := make([]binlogObject, 0)
	seen := make(map[int]bool)
	for _, object := range plan.SourceObjects() {
		if !strings.HasPrefix(object.GetName(), BinlogPath) {
			continue
		}
		logicalName := copy.StripCompressionExtension(path.Base(object.GetName()))
		if !strings.HasPrefix(logicalName, prefix+".") {
			continue
		}
		number := BinlogNum(logicalName)
		if number < startNum {
			continue
		}
		objects = append(objects, binlogObject{path: object.GetName(), name: logicalName, num: number})
		seen[number] = true
	}
	if len(objects) == 0 {
		return fmt.Errorf("no MySQL binlogs found at or after %q", start)
	}
	slices.SortFunc(objects, func(a, b binlogObject) int { return a.num - b.num })
	for number := startNum; number <= objects[len(objects)-1].num; number++ {
		if !seen[number] {
			return fmt.Errorf("gap in MySQL binlog history: %s.%d is missing", prefix, number)
		}
	}
	for _, object := range objects {
		if err := plan.AddObject(object.path, object.path, copy.PayloadPhase, false); err != nil {
			return err
		}
	}
	return nil
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
