package models

import (
	"fmt"
	"regexp"

	"github.com/wal-g/wal-g/utility"
)

// Archive path constants.
const (
	OplogArchBasePath   = "oplog_" + utility.VersionStr + "/"
	ArchNameTSDelimiter = "_"
	ArchiveTypeOplog    = "oplog"
	ArchiveTypeGap      = "gap"
)

var (
	ArchRegexp = regexp.MustCompile(`^(oplog|gap)_(?P<startTS>[0-9]+\.[0-9]+)_(?P<endTS>[0-9]+\.[0-9]+)\.(?P<Ext>[^$]+)$`)
)

// Archive defines oplog archive representation.
type Archive struct {
	Start Timestamp
	End   Timestamp
	Ext   string
	Type  string
}

// NewArchive builds Archive struct with given arguments.
func NewArchive(start, end Timestamp, ext, atype string) (Archive, error) {
	if LessTS(end, start) {
		return Archive{}, fmt.Errorf("malformed archive, Start timestamp < End timestamp: %s < %s", start, end)
	}
	if atype != ArchiveTypeOplog && atype != ArchiveTypeGap {
		return Archive{}, fmt.Errorf("malformed archive, unknown type: %s", atype)
	}

	return Archive{start, end, ext, atype}, nil
}

// In returns if oplog with given timestamp is exists in archive.
func (a Archive) In(ts Timestamp) bool {
	return (LessTS(a.Start, ts) && LessTS(ts, a.End)) || a.End == ts
}

// Filename builds archive filename from timestamps, extension and type.
// example: oplog_1569009857.10_1569009101.99.lzma
func (a Archive) Filename() string {
	return fmt.Sprintf("%s_%v%s%v.%s", a.Type, a.Start, ArchNameTSDelimiter, a.End, a.Ext)
}

// Extension returns extension of archive file name.
func (a Archive) Extension() string {
	return a.Ext
}

// ArchFromFilename builds Arch from given path.
// TODO: support empty extension
func ArchFromFilename(path string) (Archive, error) {
	res := ArchRegexp.FindAllStringSubmatch(path, -1)
	if len(res) != 1 {
		return Archive{}, fmt.Errorf("can not parse oplog path: %s", path)
	}
	match := res[0]

	archiveType := match[1]
	startTS, startErr := TimestampFromStr(match[2])
	endTS, endErr := TimestampFromStr(match[3])
	ext := match[4]
	if startErr != nil || endErr != nil {
		return Archive{}, fmt.Errorf("can not parse oplog path timestamps: %s", path)
	}
	return NewArchive(startTS, endTS, ext, archiveType)
}
