package archive

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
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

	var seqStart *models.Archive
	var seqEnd *models.Archive
	lastTSArch := make(map[models.Timestamp]*models.Archive)

	for i := range archives {
		arch := archives[i]
		if arch.Type != models.ArchiveTypeOplog {
			continue
		}
		if _, ok := lastTSArch[arch.End]; ok {
			return nil, errors.Errorf("duplicate archives with the same end %+v (archives: %+v)", arch.End, archives)
		}
		lastTSArch[arch.End] = &arch
		if seqStart == nil && arch.In(since) {
			seqStart = &arch
		}
		if seqEnd == nil && arch.In(until) {
			seqEnd = &arch
		}
	}
	if seqStart == nil {
		return nil, fmt.Errorf("can not find archive with since timestamp '%s'", since)
	}
	if seqEnd == nil {
		return nil, fmt.Errorf("can not find archive with until timestamp '%s'", until)
	}

	archPath := Sequence{}
	ok := true
	ts := models.Timestamp{}
	for i := 0; ok && i <= len(archives); i++ {
		archPath = append(archPath, *seqEnd)
		if seqEnd.In(since) {
			archPath.Reverse()
			return archPath, nil
		}
		ts = seqEnd.Start
		seqEnd, ok = lastTSArch[ts]
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
func BackupNamesFromBackups(backups []*models.Backup) []string {
	names := make([]string, 0, len(backups))
	for _, backup := range backups {
		names = append(names, backup.BackupName)
	}
	return names
}

// LastKnownInBackupTS returns begin_ts of oldest backup
func LastKnownInBackupTS(backups []*models.Backup) (models.Timestamp, error) {
	if len(backups) == 0 {
		return models.Timestamp{}, fmt.Errorf("empty backups list given")
	}
	minTS := backups[0].MongoMeta.Before.LastMajTS
	for i := 1; i < len(backups); i++ {
		ts := backups[i].MongoMeta.Before.LastMajTS
		if models.LessTS(ts, minTS) {
			minTS = ts
		}
	}
	return minTS, nil
}

func SplitMongoBackups(backups []*models.Backup, purgeBackups, retainBackups map[string]bool) (purge, retain []*models.Backup) {
	for _, backup := range backups {
		if purgeBackups[backup.Name()] {
			purge = append(purge, backup)
			continue
		}
		if retainBackups[backup.Name()] {
			retain = append(retain, backup)
		}
	}
	return purge, retain
}

func MongoModelToTimedBackup(backups []*models.Backup) []internal.TimedBackup {
	if backups == nil {
		return nil
	}
	result := make([]internal.TimedBackup, len(backups))
	for i := range backups {
		result[i] = backups[i]
	}
	return result
}

// SplitPurgingOplogArchivesByTS returns archives with start_maj_ts < purgeBeforeTS.
func SplitPurgingOplogArchivesByTS(archives []models.Archive, purgeBeforeTS models.Timestamp) []models.Archive {
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

//OldestBackupAfterTime returns last backup after given time.
func OldestBackupAfterTime(backups []*models.Backup, after time.Time) (*models.Backup, error) {
	if len(backups) <= 0 {
		return nil, fmt.Errorf("empty backup list received")
	}
	retainAfterTS := after.Unix()

	oldestBackup := backups[0]
	fromRetain := oldestBackup.FinishLocalTime.Unix() - retainAfterTS
	if fromRetain < 0 { // retain point is in future
		return nil, fmt.Errorf("no backups newer than retain point")
	}

	for _, curBackup := range backups {
		curFromRetain := curBackup.FinishLocalTime.Unix() - retainAfterTS
		if curFromRetain > fromRetain {
			return nil, fmt.Errorf("backups are not sorted by finish time")
		}
		if curFromRetain < 0 {
			return oldestBackup, nil
		}
		fromRetain = curFromRetain
		oldestBackup = curBackup
	}
	return oldestBackup, nil
}

// SelectPurgingOplogArchives builds archive list to be deleted.
func SelectPurgingOplogArchives(archives []models.Archive,
	backups []*models.Backup,
	retainAfterTS *models.Timestamp) []models.Archive {
	var purgeArchives []models.Archive
	var arch models.Archive
	for i := range archives {
		arch = archives[i]

		// retain if arch is in pitr period
		if retainAfterTS != nil && models.LessTS(*retainAfterTS, arch.End) { // TODO: check ts is set
			tracelog.DebugLogger.Printf(
				"Keeping oplog archive due to retain timestamp (%+v): %s", retainAfterTS, arch.Filename())
			continue
		}

		// retain if arch is part of backup
		if backup := models.FirstOverlappingBackupForArch(arch, backups); backup != nil {
			tracelog.DebugLogger.Printf(
				"Keeping oplog archive due to overlapping with backup (%+v): %s", backup, arch.Filename())
			continue
		}
		purgeArchives = append(purgeArchives, arch)
	}
	return purgeArchives
}
