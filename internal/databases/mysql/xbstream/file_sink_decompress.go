package xbstream

import (
	"fmt"
	"io"
	"os"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"github.com/wal-g/wal-g/internal/splitmerge"
	"github.com/wal-g/wal-g/utility"
)

type fileSinkDecompress struct {
	dataDir       string
	file          *os.File
	writeHere     chan []byte
	fileCloseChan chan struct{}
	xbOffset      uint64
}

var _ fileSink = &fileSinkDecompress{}

func newFileSinkDecompress(filePath string, dataDir string, decompressor compression.Decompressor) fileSink {
	// xbstream is a simple archive format. Compression / encryption / delta-files are xtrabackup features.
	// so, all chunks of one compressed file is a _single_ stream
	// we should combine data from all file chunks in a single io.Reader before passing to Decompressor:
	writeHere := make(chan []byte)
	readHere, err := decompressor.Decompress(splitmerge.NewChannelReader(writeHere))
	tracelog.ErrorLogger.FatalfOnError("Cannot decompress: %v", err)

	file, err := safeFileCreate(dataDir, filePath)
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)

	sink := &fileSinkDecompress{
		dataDir:       dataDir,
		file:          file,
		writeHere:     writeHere,
		fileCloseChan: make(chan struct{}),
		xbOffset:      uint64(0),
	}

	go func() {
		_, err := io.Copy(file, readHere)
		tracelog.ErrorLogger.FatalfOnError("Cannot copy data: %v", err)
		err = innodb.RepairSparse(file)
		if err != nil {
			tracelog.WarningLogger.Printf("Error during repairSparse(): %v", err)
		}
		utility.LoggedClose(file, "datasink.Close()")
		close(sink.fileCloseChan)
	}()

	return sink
}

func (sink *fileSinkDecompress) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF {
		close(sink.writeHere)
		<-sink.fileCloseChan // file will be closed in goroutine, wait for it...
		return ErrSinkEOF
	}

	if len(chunk.SparseMap) != 0 {
		tracelog.ErrorLogger.Fatalf("Found compressed file %v with sparse map", chunk.Path)
	}
	if sink.xbOffset != chunk.Offset {
		tracelog.ErrorLogger.Fatalf("Offset mismatch for file %v: expected=%v, actual=%v", chunk.Path, sink.xbOffset, chunk.Offset)
	}
	sink.xbOffset += chunk.PayloadLen

	// synchronously read data & send it to writer
	buffer := make([]byte, chunk.PayloadLen)
	_, err := io.ReadFull(chunk, buffer)
	tracelog.ErrorLogger.FatalfOnError(fmt.Sprintf("ReadFull on file %v", chunk.Path), err)
	sink.writeHere <- buffer
	return nil
}
