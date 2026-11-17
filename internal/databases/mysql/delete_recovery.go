package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func loadDeletionBackup(ctx context.Context, root storage.Folder, name string) (internal.DeletionBackup, error) {
	var s StreamSentinelDto
	doc, err := internal.ReadDeletionMetadata(ctx, root.GetSubFolder(utility.BaseBackupPath), name+utility.SentinelSuffix, &s)
	if err != nil {
		return internal.DeletionBackup{}, err
	}
	b := internal.DeletionBackup{Name: name, Problem: doc.Problem, Metadata: s, FinishTime: s.StopLocalTime}
	b.Permanent, b.ProtectionKnown = doc.Bool("IsPermanent")
	_, known := doc.Bool("IsIncremental")
	incErr := validateDeletionIncrement(name, &s)
	if !known {
		incErr = errors.Join(incErr, fmt.Errorf("inconsistent IsIncremental in %s", name))
	}
	b.Problem = errors.Join(b.Problem, incErr)
	b.DependenciesKnown = doc.Problem == nil && incErr == nil
	if b.DependenciesKnown && s.IncrementFrom != nil {
		b.Parent, b.Base = *s.IncrementFrom, *s.IncrementFullName
	}
	return b, nil
}

// DeleteUnusedLogs is not supported for MySQL
func PlanRecoveryDeletion(ctx context.Context, root storage.Folder, destination string,
	names []string, _ bool) (*internal.DeletionPlan, error) {
	isBackupName := func(name string) bool { return strings.HasPrefix(name, "stream_") }
	p, err := internal.NewDeletionPlan(ctx, root, destination, names, true, isBackupName, loadDeletionBackup)
	if err != nil {
		return p, err
	}
	if err := p.PrepareTargets(ctx, nil); err != nil {
		return p, err
	}
	return p, internal.PlanDeletionJournals(ctx, p, BinlogPath)
}
