package xbstream

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
)

// DiffBackupSink doesn't try to replicate sophisticated xtrabackup logic
// instead, we do following:
// * extract all non-diff files to incrementalDir
// * apply diff-files to dataDir 'inplace' + add truncated versions of diff-files to incrementalDir
// * let xtrabackup do its job
func DiffBackupSink(stream *Reader, dataDir string, incrementalDir string) {
	err := os.MkdirAll(dataDir, 0777) // FIXME: permission & UMASK
	tracelog.ErrorLogger.FatalOnError(err)

	spaceIDCollector, err := innodb.NewSpaceIDCollector(dataDir)
	tracelog.ErrorLogger.FatalOnError(err)

	factory := fileSinkFactory{
		dataDir:          dataDir,
		incrementalDir:   incrementalDir,
		decompress:       true, // always decompress files when diff-files applied
		inplace:          true,
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
			tracelog.ErrorLogger.Fatalf("Error in chunk %v: %v", chunk.Path, err)
		}
	}

	for path := range sinks {
		tracelog.WarningLogger.Printf("File %v wasn't clossed properly. Probably xbstream is broken", path)
	}
}

// Deprecated: name doesnt match actual behavior
func AsyncDiffBackupSink(wg *sync.WaitGroup, stream *Reader, dataDir string, incrementalDir string) {
	defer wg.Done()
	DiffBackupSink(stream, dataDir, incrementalDir)
}
