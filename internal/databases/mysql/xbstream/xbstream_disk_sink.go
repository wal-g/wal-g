package xbstream

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/splitmerge"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type dataSink interface {
	io.Closer
	Process(chunk *Chunk)
}

type simpleFileSink struct {
	file *os.File
}

var _ dataSink = &simpleFileSink{}

func newSimpleFileSink(file *os.File) dataSink {
	return &simpleFileSink{file}
}

func (sink *simpleFileSink) Close() error {
	return sink.file.Close()
}

func (sink *simpleFileSink) Process(chunk *Chunk) {
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
			tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
			_, err = io.CopyN(sink.file, chunk, int64(schunk.WriteBytes))
			tracelog.ErrorLogger.FatalfOnError("copyN: %v", err)
		}
	}
}

type decompressFileSink struct {
	file          *os.File
	writeHere     chan []byte
	fileCloseChan chan struct{}
	xbOffset      uint64
}

var _ dataSink = &decompressFileSink{}

func newDecompressFileSink(file *os.File, decompressor compression.Decompressor) dataSink {
	// xbstream is a simple archive format. Compression / encryption / delta-files are xtrabackup features.
	// so, all chunks of one compressed file is a _single_ stream
	// we should combine data from all file chunks in a single io.Reader before passing to Decompressor:
	writeHere := make(chan []byte)
	fileCloseChan := make(chan struct{})
	reader := splitmerge.NewChannelReader(writeHere)
	readHere, err := decompressor.Decompress(reader)
	tracelog.ErrorLogger.FatalfOnError("Cannot decompress: %v", err)

	go func() {
		_, err := io.Copy(file, readHere)
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("Cannot copy data: %v", err)
		}
		utility.LoggedClose(file, "datasink.Close()")
		close(fileCloseChan)

	}()

	return &decompressFileSink{
		file:          file,
		writeHere:     writeHere,
		fileCloseChan: fileCloseChan,
	}
}

func (sink *decompressFileSink) Close() error {
	close(sink.writeHere)
	<-sink.fileCloseChan // file will be closed in goroutine, wait for it...
	return nil
}

func (sink *decompressFileSink) Process(chunk *Chunk) {
	// FIXME: check whether encrypted or compressed fields doesn't support Sparse writes
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
	tracelog.ErrorLogger.FatalfOnError("ReadFull %v", err)
	sink.writeHere <- buffer
}

type dataSinkFactory struct {
	output     string
	decompress bool
}

func (dsf *dataSinkFactory) MapDataSinkPath(path string) string {
	ext := filepath.Ext(path)
	if dsf.decompress {
		if ext == ".lz4" || ext == ".zst" {
			return strings.TrimSuffix(path, ext)
		}
		if ext == ".qp" {
			tracelog.WarningLogger.Print("qpress not supported.")
		}
	}
	return path
}

func (dsf *dataSinkFactory) NewDataSink(path string) dataSink {
	var err error
	ext := filepath.Ext(path)
	path = dsf.MapDataSinkPath(path)

	filePath := filepath.Join(dsf.output, path)
	err = os.MkdirAll(filepath.Dir(filePath), 0777)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)

	if dsf.decompress {
		decompressor := compression.FindDecompressor(ext)
		if decompressor != nil {
			return newDecompressFileSink(file, decompressor)
		}
	}

	return newSimpleFileSink(file)
}

// xbstream Disk Sink will unpack archive to disk.
// Note: files may be compressed(quicklz,lz4,zstd) / encrypted("NONE", "AES128", "AES192","AES256")
func DiskSink(stream *Reader, output string, decompress bool) {
	err := os.MkdirAll(output, 0777)
	tracelog.ErrorLogger.FatalOnError(err)

	factory := dataSinkFactory{output, decompress}

	sinks := make(map[string]dataSink)
	for {
		chunk, err := stream.Next()
		if err == io.EOF {
			break
		}
		tracelog.ErrorLogger.FatalfOnError("Cannot read next chunk: %v", err)

		path := factory.MapDataSinkPath(chunk.Path)
		sink, ok := sinks[path]
		if !ok {
			sink = factory.NewDataSink(chunk.Path)
			sinks[path] = sink
			tracelog.DebugLogger.Printf("Extracting %v", chunk.Path)
		}

		if chunk.Type == ChunkTypeEOF {
			utility.LoggedClose(sink, "datasink.Close()")
			delete(sinks, path)
			continue
		}

		sink.Process(chunk)
	}

	for path, sink := range sinks {
		tracelog.WarningLogger.Printf("File %v wasn't clossed properly. Probably xbstream is broken", path)
		utility.LoggedClose(sink, "datasink.Close()")
	}
}
