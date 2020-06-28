package archive

import (
	"fmt"
	"sort"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/wal-g/tracelog"
)

// Sequence represents serial archive route
type Sequence []models.Archive

// Reverse sorts Sequence
func (p Sequence) Reverse() {
	for i, j := 0, len(p)-1; i < j; i, j = i+1, j-1 {
		p[i], p[j] = p[j], p[i]
	}
}

// SequenceBetweenTS builds archive order between since and until timestamps
// archives can be written since multiple nodes and overlap each over,
// some timestamps may be lost, we should detect these cases
func SequenceBetweenTS(archives []models.Archive, since, until models.Timestamp) (Sequence, error) {
	if models.LessTS(until, since) {
		return nil, fmt.Errorf("until ts must be greater or equal to since ts")
	}

	var seqEnd *models.Archive
	lastTSArch := make(map[models.Timestamp]*models.Archive)

	for i := range archives {
		arch := archives[i]
		if arch.Type != models.ArchiveTypeOplog {
			continue
		}
		lastTSArch[arch.End] = &arch // TODO: we can have few archives with same endTS
		if seqEnd == nil && arch.In(until) {
			seqEnd = &arch
		}
	}
	if seqEnd == nil {
		return nil, fmt.Errorf("can not find archive with until timestamp '%s'", until)
	}

	archPath := Sequence{}
	ok := true
	i := 0
	ts := models.Timestamp{}
	for ok && i <= len(archives) {
		archPath = append(archPath, *seqEnd)
		if seqEnd.In(since) {
			archPath.Reverse()
			return archPath, nil
		}
		ts = seqEnd.Start
		seqEnd, ok = lastTSArch[ts]
		i++
	}
	if !ok {
		return nil, fmt.Errorf("previous archive in sequence with last ts '%s' does not exist", ts)
	}
	return nil, fmt.Errorf("cycles in archive sequence detected")
}

// BackupNamesFromBackupTimes forms list of backup names from BackupTime
func BackupNamesFromBackupTimes(backups []internal.BackupTime) []string {
	names := make([]string, 0, len(backups))
	for _, b := range backups {
		names = append(names, b.BackupName)
	}
	return names
}

// BackupNamesFromBackups forms list of backup names from Backups
func BackupNamesFromBackups(backups []Backup) []string {
	names := make([]string, 0, len(backups))
	for _, b := range backups {
		names = append(names, b.BackupName)
	}
	return names
}

// LastKnownInBackupTS returns begin_ts of oldest backup
func LastKnownInBackupTS(backups []Backup) (models.Timestamp, error) {
	if len(backups) == 0 {
		return models.Timestamp{}, fmt.Errorf("empty backups list given")
	}
	minTs := backups[0].MongoMeta.Before.LastMajTS
	for i := 1; i < len(backups); i++ {
		ts := backups[i].MongoMeta.Before.LastMajTS
		if models.LessTS(ts, minTs) {
			minTs = ts
		}
	}
	return minTs, nil
}

// SplitPurgingBackups partitions backups to delete and retain
func SplitPurgingBackups(backups []Backup, retainCount *int, retainAfter *time.Time) (purge []Backup, retain []Backup, err error) {
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartLocalTime.After(backups[j].StartLocalTime)
	})

	for _, backup := range backups {
		if retainCount != nil && len(retain) < *retainCount { // TODO: fix condition, use func args
			tracelog.DebugLogger.Printf("Preserving backup per retain count policy: %s", backup.BackupName)
			retain = append(retain, backup)
			continue
		}

		if retainAfter != nil && backup.StartLocalTime.After(*retainAfter) { // TODO: fix condition, use func args
			tracelog.DebugLogger.Printf("Preserving backup per retain time policy: %s", backup.BackupName)
			retain = append(retain, backup)
			continue
		}
		purge = append(purge, backup)
	}
	return purge, retain, nil
}

// SplitPurgingOplogArchives deletes archives with start_maj_ts < purgeBeforeTS
func SplitPurgingOplogArchives(archives []models.Archive, purgeBeforeTS models.Timestamp) []models.Archive {
	purge := make([]models.Archive, 0)
	for _, arch := range archives {
		if models.LessTS(arch.End, purgeBeforeTS) {
			tracelog.DebugLogger.Printf("Purging oplog archive: %s", arch.Filename())
			purge = append(purge, arch)
		} else {
			tracelog.DebugLogger.Printf("Keeping oplog archive: %s", arch.Filename())
		}
	}
	return purge
}
