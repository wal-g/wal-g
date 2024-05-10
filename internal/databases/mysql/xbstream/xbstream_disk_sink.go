package xbstream

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"io"
	"os"
	"path/filepath"
)

// xbstream Disk Sink will unpack archive to disk.
// Note: files may be compressed(quicklz,lz4,zstd) / encrypted("NONE", "AES128", "AES192","AES256")
func DiskSink(stream *Reader, output string) {
	err := os.MkdirAll(output, 0777)
	tracelog.ErrorLogger.FatalOnError(err)

	files := make(map[string]*os.File)

	for {
		chunk, err := stream.Next()
		if err == io.EOF {
			break
		}
		tracelog.ErrorLogger.FatalfOnError("Cannot read next chunk: %v", err)

		path := chunk.Path
		file, ok := files[path]
		if !ok {
			filePath := filepath.Join(output, path)
			err = os.MkdirAll(filepath.Dir(filePath), 0777)
			tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)

			file, err = os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			files[path] = file
			tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)
		}

		if chunk.Type == ChunkTypeEOF {
			file.Close()
			delete(files, path)
			continue
		}

		_, err = file.Seek(int64(chunk.Offset), io.SeekStart)
		tracelog.ErrorLogger.FatalfOnError("seek: %v", err)

		if len(chunk.SparseMap) == 0 {
			_, err = io.Copy(file, chunk)
			tracelog.ErrorLogger.FatalfOnError("copy %v", err)
		} else {
			for _, schunk := range chunk.SparseMap {
				off, err := file.Seek(int64(schunk.SkipBytes), io.SeekCurrent)
				tracelog.ErrorLogger.FatalfOnError("seek: %v", err)
				err = ioextensions.PunchHole(file, off-int64(schunk.SkipBytes), int64(schunk.SkipBytes))
				tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
				_, err = io.CopyN(file, chunk, int64(schunk.WriteBytes))
				tracelog.ErrorLogger.FatalfOnError("copyN: %v", err)
			}
		}
	}
}
