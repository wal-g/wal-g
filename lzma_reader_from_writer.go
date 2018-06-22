package walg

import (
	"github.com/ulikunitz/xz/lzma"
	"io"
	"runtime"
)

type LzmaReaderFromWriter struct {
	lzma.Writer
	BlockMaxSize    int // the size of the decompressed data block Default=20MB.
}

func NewLzmaReaderFromWriter (dst io.Writer) (*LzmaReaderFromWriter, error) {
	lzmaWriter, err := lzma.NewWriter(dst)
	if err != nil {
		return nil, err
	}
	return &LzmaReaderFromWriter{
		Writer: *lzmaWriter,
		BlockMaxSize: DefaultDecompressedBlockMaxSize,
	}, nil
}

// ReadFrom compresses the data read from the io.Reader and writes it to the underlying io.Writer.
// Returns the number of bytes read.
// It does not close the Writer.
func (writer *LzmaReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	cpus := runtime.GOMAXPROCS(0)
	buf := make([]byte, cpus*writer.BlockMaxSize)
	for {
		m, er := io.ReadFull(reader, buf)
		n += int64(m)
		if er == nil || er == io.ErrUnexpectedEOF || er == io.EOF {
			if _, err = writer.Write(buf[:m]); err != nil {
				return
			}
			if er == nil {
				continue
			}
			return
		}
		return n, er
	}
}
