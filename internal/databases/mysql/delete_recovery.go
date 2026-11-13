package mysql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

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

func PlanRecoveryDeletion(ctx context.Context, root storage.Folder, destination string,
	names []string, logs bool) (*internal.DeletionPlan, error) {
	isBackupName := func(name string) bool { return strings.HasPrefix(name, "stream_") }
	p, err := internal.NewDeletionPlan(ctx, root, destination, names, true, isBackupName, loadDeletionBackup)
	if err != nil {
		return p, err
	}
	if err := p.PrepareTargets(ctx, nil); err != nil {
		return p, err
	}
	if logs {
		if err := planRecoveryBinlogs(ctx, p); err != nil {
			return p, err
		}
	}
	return p, internal.PlanDeletionJournals(ctx, p, BinlogPath)
}

var recoveryBinlogName = regexp.MustCompile(`^([A-Za-z0-9_-][A-Za-z0-9_.-]*)\.([0-9]+)$`)

const recoveryBinlogCodecs = "lz4|lzma|lzo|br|zst|nop|gz|gzip"

var recoveryBinlogObject = regexp.MustCompile(`^([A-Za-z0-9_-][A-Za-z0-9_.-]*\.[0-9]+)(\.(` + recoveryBinlogCodecs + `))?$`)

func parseRecoveryBinlog(name string) (string, uint64, bool) {
	m := recoveryBinlogName.FindStringSubmatch(name)
	if m == nil {
		return "", 0, false
	}
	n, err := strconv.ParseUint(m[2], 10, 64)
	return m[1], n, err == nil
}

func planRecoveryBinlogs(ctx context.Context, p *internal.DeletionPlan) error {
	retention := deletionBinlogRetention{}
	retained := p.Retained()
	for _, b := range retained {
		if err := retention.retainBackup(b); err != nil {
			p.Defer(err.Error())
			return nil
		}
	}
	if !retention.hasCutoff {
		p.Defer("binlogs retained: no ordinary retained backup establishes a boundary")
		return nil
	}
	cutoff, err := recoveryBinlogTimeCutoff(ctx, p.Root, retained)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		p.Defer("binlogs retained: unknown archive-time boundary: " + err.Error())
		return nil
	}
	p.Details = append(p.Details, fmt.Sprintf("binlogs retain %s.%d and later, other lineages, and objects modified at or after %s",
		retention.prefix, retention.cutoff, cutoff.UTC().Format(time.RFC3339Nano)))
	unknownTimestamp := false
	err = p.PlanObjects(ctx, "unneeded binlogs", p.Root.GetSubFolder(BinlogPath), func(obj storage.Object) bool {
		if !retention.eligibleName(obj.GetName()) {
			return false
		}
		if obj.GetLastModified().IsZero() {
			unknownTimestamp = true
			return false
		}
		return obj.GetLastModified().Before(cutoff)
	})
	if unknownTimestamp {
		p.Defer("binlogs retained: candidate objects have unknown modification times")
	}
	return err
}

func recoveryBinlogTimeCutoff(ctx context.Context, root storage.Folder, retained []internal.DeletionBackup) (time.Time, error) {
	base, logs := root.GetSubFolder(utility.BaseBackupPath), root.GetSubFolder(BinlogPath)
	anchors := make(map[string]time.Time)
	var cutoff time.Time
	for _, b := range retained {
		if b.Permanent {
			continue
		}
		s, ok := b.Metadata.(StreamSentinelDto)
		if !ok || s.BinLogStart == "" {
			return time.Time{}, fmt.Errorf("missing BinLogStart for %s", b.Name)
		}
		anchor, found := anchors[s.BinLogStart]
		if !found {
			var err error
			anchor, err = recoveryBinlogAnchorTime(ctx, logs, s.BinLogStart)
			if err != nil {
				return time.Time{}, fmt.Errorf("backup %s: %w", b.Name, err)
			}
			anchors[s.BinLogStart] = anchor
		}
		sentinel, err := recoveryObjectTime(ctx, base, b.Name+utility.SentinelSuffix)
		if err != nil {
			return time.Time{}, err
		}
		if sentinel.Before(anchor) {
			anchor = sentinel
		}
		if cutoff.IsZero() || anchor.Before(cutoff) {
			cutoff = anchor
		}
	}
	return cutoff, nil
}

func recoveryBinlogAnchorTime(ctx context.Context, logs storage.Folder, name string) (time.Time, error) {
	var earliest time.Time
	for _, ext := range strings.Split("|"+recoveryBinlogCodecs, "|") {
		key := name
		if ext != "" {
			key += "." + ext
		}
		modified, err := recoveryObjectTime(ctx, logs, key)
		var missing storage.ObjectNotFoundError
		if errors.As(err, &missing) {
			continue
		}
		if err != nil {
			return time.Time{}, err
		}
		if earliest.IsZero() || modified.Before(earliest) {
			earliest = modified
		}
	}
	if earliest.IsZero() {
		return time.Time{}, fmt.Errorf("start binlog %s is absent", name)
	}
	return earliest, nil
}

func recoveryObjectTime(ctx context.Context, folder storage.Folder, key string) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	obj, err := folder.StatObject(ctx, key)
	if err != nil {
		return time.Time{}, fmt.Errorf("stat %s/%s: %w", strings.TrimRight(folder.GetPath(), "/"), key, err)
	}
	if obj.GetLastModified().IsZero() {
		return time.Time{}, fmt.Errorf("unknown modification time for %s/%s", strings.TrimRight(folder.GetPath(), "/"), key)
	}
	return obj.GetLastModified(), nil
}

type deletionBinlogRetention struct {
	prefix, uuid string
	cutoff       uint64
	hasCutoff    bool
}

func (r *deletionBinlogRetention) retainBackup(b internal.DeletionBackup) error {
	if b.Permanent {
		return nil
	}
	s, ok := b.Metadata.(StreamSentinelDto)
	if !ok || b.Problem != nil || !b.ProtectionKnown {
		return fmt.Errorf("binlogs retained: unknown metadata for %s", b.Name)
	}
	lineage, first, err := r.boundaries(&s)
	if err != nil {
		return fmt.Errorf("binlogs retained: unknown or conflicting binlog lineage/boundary for %s: %w", b.Name, err)
	}
	r.prefix = lineage
	if s.ServerUUID != "" {
		r.uuid = s.ServerUUID
	}
	if !r.hasCutoff || first < r.cutoff {
		r.cutoff, r.hasCutoff = first, true
	}
	return nil
}
func (r *deletionBinlogRetention) boundaries(s *StreamSentinelDto) (string, uint64, error) {
	start := s.BinLogStart
	if start == "" {
		start = s.BinLogFileName
	}
	lineage, first, valid := parseRecoveryBinlog(start)
	endLineage, last, endValid := parseRecoveryBinlog(s.BinLogFileName)
	if !valid || !endValid || lineage != endLineage || last < first {
		return "", 0, fmt.Errorf("invalid binlog bounds")
	}
	if r.prefix != "" && r.prefix != lineage {
		return "", 0, fmt.Errorf("different binlog prefixes")
	}
	if r.uuid != "" && s.ServerUUID != "" && r.uuid != s.ServerUUID {
		return "", 0, fmt.Errorf("different server UUIDs")
	}
	return lineage, first, nil
}
func (r *deletionBinlogRetention) eligibleName(name string) bool {
	match := recoveryBinlogObject.FindStringSubmatch(strings.TrimLeft(name, "/"))
	if match == nil {
		return false
	}
	lineage, n, ok := parseRecoveryBinlog(match[1])
	if !ok || lineage != r.prefix || n >= r.cutoff {
		return false
	}

	return true
}
