package archive

import (
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/wal-g/storages/storage"
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

// SequenceBetweenTS builds archive order between begin and target timestamps
func SequenceBetweenTS(archives []models.Archive, begin, target models.Timestamp) (Sequence, error) {
	var lastArch *models.Archive
	endArch := make(map[models.Timestamp]*models.Archive)

	for _, arch := range archives {
		endArch[arch.End] = &arch
		if lastArch == nil && arch.In(target) {
			lastArch = &arch
		}
	}
	if lastArch == nil {
		return nil, fmt.Errorf("can not find archive with target timestamp %s", target)
	}

	archPath := make(Sequence, 0, len(endArch))
	ok := true
	for ok { // TODO: detect cycles
		archPath = append(archPath, *lastArch)
		if lastArch.In(begin) {
			archPath.Reverse()
			return archPath, nil
		}
		ts := lastArch.Start
		lastArch, ok = endArch[ts]
	}
	return nil, fmt.Errorf("previous archive with starting ts '%s' does not exist", begin)
}

// ArchivingResumeTS returns archiving Start timestamp
func ArchivingResumeTS(folder storage.Folder) (models.Timestamp, bool, error) {
	lastKnownTS, err := LastKnownArchiveTS(folder)
	if err != nil {
		return models.Timestamp{}, false, err
	}
	zeroTS := models.Timestamp{}
	if lastKnownTS == zeroTS {
		// TODO: add additional check
		return zeroTS, true, nil
	}
	return lastKnownTS, false, nil
}

// LastKnownArchiveTS returns the most recent existed timestamp in storage folder
func LastKnownArchiveTS(folder storage.Folder) (models.Timestamp, error) {
	maxTS := models.Timestamp{}
	keys, _, err := folder.ListFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	for _, key := range keys {
		filename := key.GetName()
		arch, err := models.ArchFromFilename(filename)
		if err != nil {
			return models.Timestamp{}, fmt.Errorf("can not build archive from filename '%s': %w", filename, err)
		}
		maxTS = models.Max(maxTS, arch.End)
	}
	return maxTS, nil
}
