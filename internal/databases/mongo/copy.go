package mongo

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func BuildCopyPlan(ctx context.Context, from, to storage.Folder, backupName string, withHistory bool) (*copyutil.Plan, error) {
	plan, err := copyutil.NewPlan(ctx, from, to)
	if err != nil {
		return nil, err
	}
	names, err := plan.ResolveBackupNames(ctx, backupName)
	if err != nil {
		return nil, err
	}

	var historyStart models.Timestamp
	for i, name := range names {
		if err := plan.AddBackup(name, name); err != nil {
			return nil, err
		}
		if !withHistory {
			continue
		}
		sentinel, err := common.DownloadSentinel(ctx, from.GetSubFolder(utility.BaseBackupPath), name)
		if err != nil {
			return nil, fmt.Errorf("read MongoDB backup %q sentinel: %w", name, err)
		}
		start := sentinel.MongoMeta.After.LastMajTS
		if sentinel.BackupType == common.LogicalBackupType {
			start = sentinel.MongoMeta.Before.LastMajTS
		}
		if sentinel.BackupType == common.BinaryBackupType && sentinel.MongoMeta.BackupLastTS.T != 0 {
			start = models.TimestampFromBson(sentinel.MongoMeta.BackupLastTS)
		}
		if i == 0 || models.LessTS(start, historyStart) {
			historyStart = start
		}
	}

	if withHistory {
		if historyStart == (models.Timestamp{}) {
			return nil, fmt.Errorf("selected MongoDB backup has no recovery timestamp")
		}
		if err := addMongoHistory(plan, historyStart); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func addMongoHistory(plan *copyutil.Plan, since models.Timestamp) error {
	archives := make([]models.Archive, 0)
	latest := models.Timestamp{}
	for _, object := range plan.SourceObjects() {
		name := object.GetName()
		if !strings.HasPrefix(name, models.OplogArchBasePath) {
			continue
		}
		archiveObject, err := models.ArchFromFilename(path.Base(name))
		if err != nil {
			return fmt.Errorf("parse MongoDB archive %q: %w", name, err)
		}
		archives = append(archives, archiveObject)
		if archiveObject.Type == models.ArchiveTypeOplog && models.LessTS(latest, archiveObject.End) {
			latest = archiveObject.End
		}
	}
	if latest == (models.Timestamp{}) {
		return fmt.Errorf("no MongoDB oplog archives found")
	}

	sequence, err := archive.SequenceBetweenTS(archives, since, latest)
	if err != nil {
		return fmt.Errorf("build continuous MongoDB oplog sequence from %s through %s: %w", since, latest, err)
	}
	for _, archiveObject := range sequence {
		name := path.Join(strings.TrimSuffix(models.OplogArchBasePath, "/"), archiveObject.Filename())
		if err := plan.AddObject(name, name, copyutil.PayloadPhase, false); err != nil {
			return err
		}
	}
	return nil
}

func HandleCopy(ctx context.Context, fromConfigFile, toConfigFile, backupName string, withHistory bool) {
	from, err := internal.StorageFromConfig(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, err := internal.StorageFromConfig(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := BuildCopyPlan(ctx, from.RootFolder(), to.RootFolder(), backupName, withHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copyutil.ExecuteRaw(ctx, plan))
}
