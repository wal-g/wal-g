package xbstream

import (
	"errors"
	"io"
	"os"
	"syscall"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

type fileSinkSimple struct {
	dataDir string
	file    *os.File
}

var _ fileSink = &fileSinkSimple{}

func newSimpleFileSink(filePath string, dataDir string) fileSink {
	file, err := safeFileCreate(dataDir, filePath)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)
	return &fileSinkSimple{
		dataDir: dataDir,
		file:    file,
	}
}

func (sink *fileSinkSimple) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF {
		utility.LoggedClose(sink.file, "")
		return ErrSinkEOF
	}

	_, err := sink.file.Seek(int64(chunk.Offset), io.SeekStart)
	tracelog.ErrorLogger.FatalfOnError("seek: %v", err)

	if len(chunk.SparseMap) == 0 {
		_, err = io.Copy(sink.file, chunk)
		tracelog.ErrorLogger.FatalfOnError("copy %v", err)
	} else {
		for _, schunk := range chunk.SparseMap {
			off, err := sink.file.Seek(int64(schunk.SkipBytes), io.SeekCurrent)
			tracelog.ErrorLogger.FatalfOnError("seek: %v", err)
			err = ioextensions.PunchHole(sink.file, off-int64(schunk.SkipBytes), int64(schunk.SkipBytes))
			if !errors.Is(err, syscall.EOPNOTSUPP) {
				tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
			}
			_, err = io.CopyN(sink.file, chunk, int64(schunk.WriteBytes))
			tracelog.ErrorLogger.FatalfOnError("copyN: %v", err)
		}
	}

	return nil
}
