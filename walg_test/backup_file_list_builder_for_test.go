package walg

import (
	"github.com/wal-g/wal-g"
	"time"
)

const (
	SimplePath      = "/simple"
	SkippedPath     = "/skipped"
	IncrementedPath = "/incremented"
)

var SimpleDescription = *walg.NewBackupFileDescription(false, false, time.Time{})
var SkippedDescription = *walg.NewBackupFileDescription(false, true, time.Time{})
var IncrementedDescription = *walg.NewBackupFileDescription(true, false, time.Time{})

type BackupFileListBuilder struct {
	fileList walg.BackupFileList
}

func NewBackupFileListBuilder() BackupFileListBuilder {
	return BackupFileListBuilder{walg.BackupFileList{}}
}

func (listBuilder BackupFileListBuilder) WithSimple() BackupFileListBuilder {
	listBuilder.fileList[SimplePath] = SimpleDescription
	return listBuilder
}

func (listBuilder BackupFileListBuilder) WithSkipped() BackupFileListBuilder {
	listBuilder.fileList[SkippedPath] = SkippedDescription
	return listBuilder
}

func (listBuilder BackupFileListBuilder) WithIncremented() BackupFileListBuilder {
	listBuilder.fileList[IncrementedPath] = IncrementedDescription
	return listBuilder
}

func (listBuilder BackupFileListBuilder) Build() walg.BackupFileList {
	return listBuilder.fileList
}
