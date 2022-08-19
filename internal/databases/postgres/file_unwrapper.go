package postgres

import (
	"archive/tar"
	"io"
	"os"
)

type FileUnwrapperType int
type FileUnwrapResultType int

const (
	DefaultBackupFileUnwrapper FileUnwrapperType = iota + 1
	CatchupBackupFileUnwrapper
)

const (
	Completed FileUnwrapResultType = iota + 1
	CreatedFromIncrement
	WroteIncrementBlocks
	Skipped
)

func NewFileUnwrapper(unwrapperType FileUnwrapperType, options *BackupFileOptions) IBackupFileUnwrapper {
	switch unwrapperType {
	case DefaultBackupFileUnwrapper:
		return &DefaultFileUnwrapper{BackupFileUnwrapper{options}}
	case CatchupBackupFileUnwrapper:
		return &CatchupFileUnwrapper{BackupFileUnwrapper{options}}
	default:
		return &DefaultFileUnwrapper{BackupFileUnwrapper{options}}
	}
}

type FileUnwrapResult struct {
	FileUnwrapResultType
	blockCount int64
}

func NewCompletedResult() *FileUnwrapResult {
	return &FileUnwrapResult{Completed, 0}
}

func NewCreatedFromIncrementResult(missingBlockCount int64) *FileUnwrapResult {
	return &FileUnwrapResult{CreatedFromIncrement, missingBlockCount}
}

func NewWroteIncrementBlocksResult(restoredBlockCount int64) *FileUnwrapResult {
	return &FileUnwrapResult{WroteIncrementBlocks, restoredBlockCount}
}

func NewSkippedResult() *FileUnwrapResult {
	return &FileUnwrapResult{Skipped, 0}
}

type BackupFileOptions struct {
	isIncremented bool
	isPageFile    bool
}

type IBackupFileUnwrapper interface {
	UnwrapNewFile(reader io.Reader, header *tar.Header, file *os.File, fsync bool) (*FileUnwrapResult, error)
	UnwrapExistingFile(reader io.Reader, header *tar.Header, file *os.File, fsync bool) (*FileUnwrapResult, error)
}

type BackupFileUnwrapper struct {
	options *BackupFileOptions
}
