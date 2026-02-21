package xbstream

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"github.com/wal-g/wal-g/internal/logging"
)

// xbstream BackupSink will unpack archive to disk.
// Note: files may be compressed(quicklz,lz4,zstd) / encrypted("NONE", "AES128", "AES192","AES256")
func BackupSink(stream *Reader, output string, decompress bool) {
	err := os.MkdirAll(output, 0777) // FIXME: permission & UMASK
	logging.FatalOnError(err)

	spaceIDCollector, err := innodb.NewSpaceIDCollector(output)
	logging.FatalOnError(err)

	factory := fileSinkFactory{
		dataDir:          output,
		incrementalDir:   "",
		decompress:       decompress,
		inplace:          false,
		spaceIDCollector: spaceIDCollector,
	}

	sinks := make(map[string]fileSink)
	for {
		chunk, err := stream.Next()
		if err == io.EOF {
			break
		}
		tracelog.ErrorLogger.FatalfOnError("Cannot read next chunk: %v", err)

		dsKey := factory.MapDataSinkKey(chunk.Path)
		sink, ok := sinks[dsKey]
		if !ok {
			sink = factory.NewDataSink(chunk.Path)
			sinks[dsKey] = sink
		}

		err = sink.Process(chunk)
		if errors.Is(err, ErrSinkEOF) {
			delete(sinks, dsKey)
		} else if err != nil {
			tracelog.ErrorLogger.Printf("Error in chunk %v: %v", chunk.Path, err)
		}
	}

	for path := range sinks {
		slog.Warn(fmt.Sprintf("File %v wasn't clossed properly. Probably xbstream is broken", path))
	}
}

func AsyncBackupSink(wg *sync.WaitGroup, stream *Reader, dataDir string, decompress bool) {
	defer wg.Done()
	BackupSink(stream, dataDir, decompress)
}
