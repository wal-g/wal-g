package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DeletionBackupMetadata struct {
	Sentinel  BackupSentinelDto
	Meta      ExtendedMetadataDto
	LogsKnown bool
	Identity  interface{}
}

func LoadDeletionBackup(ctx context.Context, root storage.Folder, name string) (internal.DeletionBackup, error) {
	base := root.GetSubFolder(utility.BaseBackupPath)
	var data DeletionBackupMetadata
	s, err := internal.ReadDeletionMetadata(ctx, base, name+utility.SentinelSuffix, &data.Sentinel)
	if err != nil {
		return internal.DeletionBackup{}, err
	}
	normalizeLegacyDeletionLSN(name, &s, &data.Sentinel)
	m, err := internal.ReadDeletionMetadata(ctx, base, name+"/metadata.json", &data.Meta)
	if err != nil {
		return internal.DeletionBackup{}, err
	}
	if m.Problem == nil {
		data.Identity = data.Meta.UserData
	}
	if data.Identity == nil && s.Problem == nil {
		data.Identity = data.Sentinel.UserData
	}
	b := reconcileDeletionProtection(name, s, m, &data)
	if s.Problem != nil {
		b.Problem = errors.Join(b.Problem, s.Problem)
	}
	incErr := internal.ValidateDeletionIncrement(name, data.Sentinel.IncrementFrom, data.Sentinel.IncrementFullName,
		data.Sentinel.IncrementCount, data.Sentinel.IncrementFromLSN != nil, true)
	b.DependenciesKnown = s.Problem == nil && incErr == nil && data.Sentinel.BackupStartLSN != nil
	b.Problem = errors.Join(b.Problem, incErr)
	if b.DependenciesKnown && data.Sentinel.IncrementFrom != nil {
		b.Parent, b.Base = *data.Sentinel.IncrementFrom, *data.Sentinel.IncrementFullName
	}
	if !b.ProtectionKnown {
		b.Problem = errors.Join(b.Problem, fmt.Errorf("unknown protection metadata for %s", name))
	}
	b.FinishTime = data.Meta.FinishTime
	data.LogsKnown = b.ProtectionKnown && b.Problem == nil && data.Meta.StartLsn > 0 && data.Meta.FinishLsn >= data.Meta.StartLsn
	b.Metadata = data
	return b, nil
}

func normalizeLegacyDeletionLSN(name string, s *internal.DeletionMetadata, sentinel *BackupSentinelDto) {
	if raw, ok := s.Fields["DeltaFromLSN"]; ok {
		var legacy *LSN
		if decodeErr := json.Unmarshal(raw, &legacy); decodeErr != nil {
			s.Problem = errors.Join(s.Problem, decodeErr)
		} else if legacy != nil {
			if sentinel.IncrementFromLSN != nil && *sentinel.IncrementFromLSN != *legacy {
				s.Problem = errors.Join(s.Problem, fmt.Errorf("conflicting incremental LSNs for %s", name))
			} else {
				sentinel.IncrementFromLSN = legacy
			}
		}
	}
}

func validV2DeletionProtection(s internal.DeletionMetadata, known bool, sentinel *BackupSentinelDto) bool {
	if s.Problem != nil || !known || sentinel.BackupStartLSN == nil || sentinel.BackupFinishLSN == nil {
		return false
	}
	return *sentinel.BackupStartLSN > 0 && *sentinel.BackupFinishLSN >= *sentinel.BackupStartLSN
}

func reconcileDeletionProtection(name string, s, m internal.DeletionMetadata, data *DeletionBackupMetadata) internal.DeletionBackup {
	b := internal.DeletionBackup{Name: name}
	compactPermanent, compactKnown := m.Bool("is_permanent")
	sentinelPermanent, sentinelKnown := s.Bool("IsPermanent")
	var version int
	_ = json.Unmarshal(s.Fields["Version"], &version)
	sentinelKnown = sentinelKnown && version == 2
	b.Permanent = compactPermanent || sentinelPermanent
	compactValid := m.Problem == nil && compactKnown
	sentinelValid := validV2DeletionProtection(s, sentinelKnown, &data.Sentinel)
	if !compactValid && sentinelValid {
		data.Meta = ExtendedMetadataDto{StartLsn: *data.Sentinel.BackupStartLSN, FinishLsn: *data.Sentinel.BackupFinishLSN,
			IsPermanent: sentinelPermanent, SystemIdentifier: data.Sentinel.SystemIdentifier}
		_ = json.Unmarshal(s.Fields["StartTime"], &data.Meta.StartTime)
		_ = json.Unmarshal(s.Fields["FinishTime"], &data.Meta.FinishTime)
	}
	b.ProtectionKnown = compactValid || sentinelValid
	if compactKnown && sentinelKnown && compactPermanent != sentinelPermanent {
		b.Problem = fmt.Errorf("conflicting permanence metadata for %s", name)
		b.ProtectionKnown = false
	}
	if compactValid {
		b.Problem = errors.Join(b.Problem, validateDeletionMetadataAgreement(name, data))
	}
	return b
}

func validateDeletionMetadataAgreement(name string, data *DeletionBackupMetadata) error {
	var problem error
	if data.Sentinel.BackupStartLSN != nil && data.Sentinel.BackupFinishLSN != nil &&
		(data.Meta.StartLsn != *data.Sentinel.BackupStartLSN || data.Meta.FinishLsn != *data.Sentinel.BackupFinishLSN) {
		problem = errors.Join(problem, fmt.Errorf("conflicting LSN metadata for %s", name))
	}
	if data.Meta.SystemIdentifier != nil && data.Sentinel.SystemIdentifier != nil &&
		*data.Meta.SystemIdentifier != *data.Sentinel.SystemIdentifier {
		problem = errors.Join(problem, fmt.Errorf("conflicting system identifier for %s", name))
	}
	return problem
}

func PlanRecoveryDeletion(ctx context.Context, root storage.Folder, destination string,
	names []string, logs bool) (*internal.DeletionPlan, error) {
	isBackupName := func(name string) bool { return strings.HasPrefix(name, "base_") }
	p, err := internal.NewDeletionPlan(ctx, root, destination, names, true, isBackupName, LoadDeletionBackup)
	if err != nil {
		return p, err
	}
	if err := p.PrepareTargets(ctx, nil); err != nil {
		return p, err
	}
	if logs {
		if err := PlanRecoveryWALs(ctx, p); err != nil {
			return p, err
		}
	}
	return p, internal.PlanDeletionJournals(ctx, p, utility.WalPath)
}

var recoveryWALKey = regexp.MustCompile(`^([0-9A-F]{24})(_delta|_part)?(\.(lz4|lzma|lzo|br|zst|nop|gzip|gz|json))?$`)
var recoveryBulkWALKey = regexp.MustCompile(`^([0-9A-F]{23})\.json$`)

func PlanRecoveryWALs(ctx context.Context, p *internal.DeletionPlan) error {
	retention := deletionWALRetention{knownTimelines: map[uint32]bool{}, segmentSize: WalSegmentSize}
	for _, b := range p.Retained() {
		if err := retention.retainBackup(p, b); err != nil {
			p.Defer(err.Error())
			return nil
		}
	}
	if !retention.hasCutoff {
		p.Defer("WALs retained: no ordinary retained backup establishes a forward recovery boundary")
		return nil
	}
	p.Details = append(p.Details, fmt.Sprintf("WALs retain segment %d and later, permanent intervals, history files and unknown timelines",
		retention.cutoff))

	return p.PlanObjects(ctx, "unneeded WALs and WAL metadata", p.Root.GetSubFolder(utility.WalPath), retention.eligible)
}

type deletionWALInterval struct {
	first, last uint64
	timeline    uint32
}
type deletionWALRetention struct {
	protected      []deletionWALInterval
	knownTimelines map[uint32]bool
	systemID       *uint64
	cutoff         uint64
	hasCutoff      bool
	segmentSize    uint64
}

func (r *deletionWALRetention) retainBackup(p *internal.DeletionPlan, b internal.DeletionBackup) error {
	data, ok := b.Metadata.(DeletionBackupMetadata)
	if !ok || !data.LogsKnown {
		return fmt.Errorf("WALs retained: unknown recovery/protection metadata for %s", b.Name)
	}
	size, err := deletionWALSegmentSize(data.Sentinel)
	if err != nil {
		return fmt.Errorf("WALs retained: WAL segment size for %s: %w", b.Name, err)
	}
	if size != r.segmentSize {
		return fmt.Errorf("WALs retained: WAL segment size for %s is %d bytes, configured %d", b.Name, size, r.segmentSize)
	}
	if data.Meta.SystemIdentifier != nil {
		if r.systemID != nil && *r.systemID != *data.Meta.SystemIdentifier {
			return errors.New("WALs retained: conflicting system identifiers")
		}
		r.systemID = data.Meta.SystemIdentifier
	}
	timeline, err := ParseTimelineFromBackupName(b.Name)
	if err != nil {
		return fmt.Errorf("WALs retained: invalid timeline for %s", b.Name)
	}
	r.knownTimelines[timeline] = true
	first := uint64(data.Meta.StartLsn-1) / r.segmentSize
	last := uint64(data.Meta.FinishLsn-1) / r.segmentSize
	if b.Permanent {
		r.protected = append(r.protected, deletionWALInterval{first, last, timeline})
	} else {
		if !r.hasCutoff || first < r.cutoff {
			r.cutoff, r.hasCutoff = first, true
		}
		if err := r.retainAncestors(p, b.Parent); err != nil {
			return err
		}
	}
	return nil
}
func (r *deletionWALRetention) retainAncestors(p *internal.DeletionPlan, parent string) error {
	for parent != "" {
		ancestor, exists := p.Backups[parent]
		if !exists {
			return fmt.Errorf("WALs retained: unknown incremental ancestor %s", parent)
		}
		ancestorData, ok := ancestor.Metadata.(DeletionBackupMetadata)
		if !ok || !ancestorData.LogsKnown {
			return fmt.Errorf("WALs retained: unknown ancestor boundary %s", parent)
		}
		start := uint64(ancestorData.Meta.StartLsn-1) / r.segmentSize
		if start < r.cutoff {
			r.cutoff = start
		}
		parent = ancestor.Parent
	}
	return nil
}
func (r *deletionWALRetention) eligible(obj storage.Object) bool {
	key := strings.TrimLeft(obj.GetName(), "/")
	if strings.Contains(key, "/") && path.Dir(key) != "wal_metadata" {
		return false
	}
	key = path.Base(key)
	var wal string
	width := uint64(1)
	if match := recoveryWALKey.FindStringSubmatch(key); match != nil {
		wal = match[1]
		if match[2] == "_delta" {
			width = 16
		}
	} else if match := recoveryBulkWALKey.FindStringSubmatch(key); match != nil {
		wal, width = match[1]+"0", 16
	} else {
		return false
	}
	timeline, number, err := parseDeletionWALFilename(wal, r.segmentSize)
	if err != nil || !r.knownTimelines[timeline] || number > ^uint64(0)-(width-1) {
		return false
	}
	last := number + width - 1
	if last >= r.cutoff {
		return false
	}
	for _, keep := range r.protected {
		if keep.timeline == timeline && number <= keep.last && last >= keep.first {
			return false
		}
	}
	return true
}

func deletionWALSegmentSize(s BackupSentinelDto) (uint64, error) {
	boundaries := []struct {
		name string
		lsn  *LSN
	}{{s.StartWalFileName, s.BackupStartLSN}, {s.EndWalFileName, s.BackupFinishLSN}}
	var size uint64
	for candidate := uint64(1 << 20); candidate <= 1<<30; candidate <<= 1 {
		matches, recorded := true, false
		for _, boundary := range boundaries {
			if boundary.name == "" {
				continue
			}
			recorded = true
			_, number, err := parseDeletionWALFilename(boundary.name, candidate)
			if err != nil || boundary.lsn == nil || *boundary.lsn == 0 ||
				number != uint64(*boundary.lsn-1)/candidate {
				matches = false
			}
		}
		if matches && recorded {
			if size != 0 {
				return 0, errors.New("ambiguous recorded WAL boundaries")
			}
			size = candidate
		}
	}
	if size == 0 {
		return 0, errors.New("missing or inconsistent recorded WAL boundaries")
	}
	return size, nil
}

func parseDeletionWALFilename(name string, segmentSize uint64) (uint32, uint64, error) {
	if len(name) != 24 || segmentSize < 1<<20 || segmentSize > 1<<30 || segmentSize&(segmentSize-1) != 0 {
		return 0, 0, fmt.Errorf("invalid WAL filename or segment size")
	}
	var parts [3]uint64
	for i := range parts {
		value, err := strconv.ParseUint(name[i*8:(i+1)*8], 16, 32)
		if err != nil {
			return 0, 0, err
		}
		parts[i] = value
	}
	perLog := uint64(1<<32) / segmentSize
	if parts[0] == 0 || parts[2] >= perLog {
		return 0, 0, fmt.Errorf("invalid WAL filename %s", name)
	}
	return uint32(parts[0]), parts[1]*perLog + parts[2], nil
}
