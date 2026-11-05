package mongo

import (
	"context"
	"fmt"
	"strings"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func PlanRecoveryDeletion(ctx context.Context, root storage.Folder, destination string,
	names []string, logs bool) (*internal.DeletionPlan, error) {
	p, err := internal.NewDeletionPlan(ctx, root, destination, names, false, func(name string) bool {
		return strings.HasPrefix(name, "stream_") || strings.HasPrefix(name, "binary_") || strings.HasPrefix(name, "base_")
	}, func(ctx context.Context, root storage.Folder, name string) (internal.DeletionBackup, error) {
		var s models.Backup
		doc, err := internal.ReadDeletionMetadata(ctx, root.GetSubFolder(utility.BaseBackupPath), name+utility.SentinelSuffix, &s)
		b := internal.DeletionBackup{DependenciesKnown: true, Problem: doc.Problem, Metadata: s, FinishTime: s.FinishLocalTime}
		b.Permanent, b.ProtectionKnown = doc.Bool("Permanent")
		if s.BackupName != "" && s.BackupName != name {
			b.Problem = fmt.Errorf("conflicting backup name %s", s.BackupName)
		}
		return b, err
	})
	if err != nil {
		return p, err
	}
	if err := p.PrepareTargets(ctx, nil); err != nil {
		return p, err
	}
	if logs {
		if err := planRecoveryOplogs(ctx, p); err != nil {
			return p, err
		}
	}
	return p, internal.PlanDeletionJournals(ctx, p, models.OplogArchBasePath)
}

func planRecoveryOplogs(ctx context.Context, p *internal.DeletionPlan) error {
	var cutoff models.Timestamp
	var backups []*models.Backup
	hasCutoff := false
	for _, b := range p.Retained() {
		s, ok := b.Metadata.(models.Backup)
		if !ok || b.Problem != nil || !b.ProtectionKnown || s.MongoMeta.Before.LastMajTS == (models.Timestamp{}) ||
			models.LessTS(s.MongoMeta.After.LastMajTS, s.MongoMeta.Before.LastMajTS) {
			p.Defer("oplogs retained: unknown recovery boundary for " + b.Name)
			return nil
		}
		backups = append(backups, &s)
		if !b.Permanent && (!hasCutoff || models.LessTS(s.MongoMeta.Before.LastMajTS, cutoff)) {
			cutoff, hasCutoff = s.MongoMeta.Before.LastMajTS, true
		}
	}
	if !hasCutoff {
		p.Defer("oplogs retained: no ordinary retained backup establishes a boundary")
		return nil
	}
	additional, err := conf.GetOplogBackupAdditionalSavingTimeSetting()
	if err != nil {
		return err
	}
	p.Details = append(p.Details, fmt.Sprintf("oplogs retain boundary %s and later, protected intervals and additional saving time", cutoff))
	return p.PlanObjects(ctx, "unneeded oplogs", p.Root.GetSubFolder(models.OplogArchBasePath), func(obj storage.Object) bool {
		a, err := models.ArchFromFilename(strings.TrimLeft(obj.GetName(), "/"))
		if err != nil || !models.LessTS(a.End, cutoff) {
			return false
		}
		return len(archive.SelectPurgingOplogArchives([]models.Archive{a}, backups, &cutoff, additional)) == 1
	})
}
