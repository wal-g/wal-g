package oplog

import (
	"fmt"
	"regexp"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

const (
	ArchNamePrefix      = "oplog_"
	ArchBasePath        = "oplog_" + utility.VersionStr + "/"
	ArchNameTSDelimiter = "_"
)

// Archive defines oplog archive representation
type Archive struct {
	start Timestamp
	end   Timestamp
	ext   string
}

// NewArchive builds Archive struct with given arguments
func NewArchive(start, end Timestamp, ext string) (*Archive, error) {
	if Less(end, start) {
		return nil, fmt.Errorf("malformed archive, start timestamp < end timestamp: %s < %s", start, end)
	}
	return &Archive{start, end, ext}, nil
}

// In returns if oplog with given timestamp is exists in archive
func (a *Archive) In(ts Timestamp) bool {
	return (Less(a.start, ts) && Less(ts, a.end)) || a.start == ts || a.end == ts
}

// Filename builds archive file ext from timestamps and extension
// example: oplog_1569009857.10_1569009101.99.lzma
func (a *Archive) Filename() string {
	return fmt.Sprintf("%s%v%s%v.%s", ArchNamePrefix, a.start, ArchNameTSDelimiter, a.end, a.ext)
}

// Extension returns extension of archive file name
func (a *Archive) Extension() string {
	return a.ext
}

// FromFilename extracts timestamps and extension from archive ext
func FromFilename(path string) (*Archive, error) {
	// TODO: add unit test and move regexp to const
	reStr := fmt.Sprintf(`%s(?P<startTS>%s)%s(?P<endTS>%s)\.(?P<ext>[^$]+)$`,
		ArchNamePrefix, TimestampRegexp, ArchNameTSDelimiter, TimestampRegexp)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, fmt.Errorf("can not compile oplog archive regexp: %w", err)
	}
	res := re.FindAllStringSubmatch(path, -1)
	for i := range res {
		startTS, startErr := TimestampFromStr(res[i][1])
		endTS, endErr := TimestampFromStr(res[i][2])
		ext := res[i][3]
		if startErr != nil || endErr != nil {
			break
		}
		return &Archive{startTS, endTS, ext}, nil
	}
	return nil, fmt.Errorf("can not parse oplog path: %s", path)
}

// ArchPath represents serial archive route
type ArchPath []*Archive

// Reverse sorts ArchPath route
func (p ArchPath) Reverse() {
	for i, j := 0, len(p)-1; i < j; i, j = i+1, j-1 {
		p[i], p[j] = p[j], p[i]
	}
}

// PathBetweenTS builds archive order between begin and target timestamps
// TODO: rename
func PathBetweenTS(folder storage.Folder, begin, target Timestamp) (ArchPath, error) {
	keys, _, err := folder.ListFolder()
	if err != nil {
		return nil, fmt.Errorf("can not list archive folder: %w", err)
	}
	var lastArch *Archive
	endArch := make(map[Timestamp]*Archive)

	for _, key := range keys {
		archName := key.GetName()
		arch, err := FromFilename(archName)
		if err != nil {
			return nil, fmt.Errorf("can not convert retrieve timestamps from oplog archive ext '%s': %w", archName, err)
		}
		endArch[arch.end] = arch
		if lastArch == nil && arch.In(target) {
			lastArch = arch
		}
	}
	if lastArch == nil {
		return nil, fmt.Errorf("can not find archive with target timestamp %s", target)
	}

	archives := make(ArchPath, 0, len(endArch))
	ok := true
	for ok { // TODO: detect cycles
		archives = append(archives, lastArch)
		if lastArch.In(begin) {
			archives.Reverse()
			return archives, nil
		}
		ts := lastArch.start
		lastArch, ok = endArch[ts]
	}
	return nil, fmt.Errorf("previous archive with starting ts '%s' does not exist", begin)
}

// ArchivingResumeTS returns archiving start timestamp
func ArchivingResumeTS(folder storage.Folder) (Timestamp, bool, error) {
	lastKnownTS, err := LastKnownArchiveTS(folder)
	if err != nil {
		return Timestamp{}, false, err
	}
	zeroTS := Timestamp{}
	if lastKnownTS == zeroTS {
		// TODO: add additional check
		return zeroTS, true, nil
	}
	return lastKnownTS, false, nil
}

// LastKnownArchiveTS returns the most recent existed timestamp in storage folder
func LastKnownArchiveTS(folder storage.Folder) (Timestamp, error) {
	maxTS := Timestamp{}
	keys, _, err := folder.ListFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	for _, key := range keys {
		filename := key.GetName()
		arch, err := FromFilename(filename)
		if err != nil {
			return Timestamp{}, fmt.Errorf("can not build archive from filename '%s': %w", filename, err)
		}
		maxTS = Max(maxTS, arch.end)
	}
	return maxTS, nil
}
