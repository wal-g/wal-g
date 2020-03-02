package models

import (
	"fmt"
	"regexp"

	"github.com/wal-g/wal-g/utility"
)

// Archive path constants
const (
	archNamePrefix      = "oplog_"
	ArchBasePath        = "oplog_" + utility.VersionStr + "/"
	ArchNameTSDelimiter = "_"
)

// Archive defines oplog archive representation
type Archive struct {
	Start Timestamp
	End   Timestamp
	Ext   string
}

// NewArchive builds Archive struct with given arguments
func NewArchive(start, end Timestamp, ext string) (Archive, error) {
	if LessTS(end, start) {
		return Archive{}, fmt.Errorf("malformed archive, Start timestamp < End timestamp: %s < %s", start, end)
	}
	return Archive{start, end, ext}, nil
}

// In returns if oplog with given timestamp is exists in archive
func (a Archive) In(ts Timestamp) bool {
	return (LessTS(a.Start, ts) && LessTS(ts, a.End)) || a.End == ts
}

// Filename builds archive filename from timestamps and extension
// example: oplog_1569009857.10_1569009101.99.lzma
func (a Archive) Filename() string {
	return fmt.Sprintf("%s%v%s%v.%s", archNamePrefix, a.Start, ArchNameTSDelimiter, a.End, a.Ext)
}

// Extension returns extension of archive file name
func (a Archive) Extension() string {
	return a.Ext
}

// ArchFromFilename builds Arch from given path
func ArchFromFilename(path string) (Archive, error) {
	// support empty extension
	reStr := fmt.Sprintf(`%s(?P<startTS>%s)%s(?P<endTS>%s)\.(?P<Ext>[^$]+)$`,
		archNamePrefix, timestampRegexp, ArchNameTSDelimiter, timestampRegexp)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return Archive{}, fmt.Errorf("can not compile oplog archive regexp: %w", err)
	}
	res := re.FindAllStringSubmatch(path, -1)
	for i := range res {
		startTS, startErr := TimestampFromStr(res[i][1])
		endTS, endErr := TimestampFromStr(res[i][2])
		ext := res[i][3]
		if startErr != nil || endErr != nil {
			break
		}
		return Archive{startTS, endTS, ext}, nil
	}
	return Archive{}, fmt.Errorf("can not parse oplog path: %s", path)
}
