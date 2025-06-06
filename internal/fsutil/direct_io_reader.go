package fsutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/ncw/directio"
	"github.com/spf13/viper"

	conf "github.com/wal-g/wal-g/internal/config"
)

const directIOBlockCount = 32

type reader struct {
	mu           *sync.Mutex
	fd           *os.File
	buff         []byte
	buffOffset   int
	alignedBlock []byte
}

// OpenReadOnlyMayBeDirectIO returns read-only io.ReadSeekCloser.
func OpenReadOnlyMayBeDirectIO(path string) (io.ReadSeekCloser, error) {
	if viper.GetBool(conf.DirectIO) {
		return NewDirectIOReadSeekCloser(path, syscall.O_RDONLY, 0)
	}
	return os.OpenFile(path, os.O_RDONLY, 0)
}

// NewDirectIOReadSeekCloser returns io.ReadSeekCloser.
func NewDirectIOReadSeekCloser(path string, flag int, perm os.FileMode) (io.ReadSeekCloser, error) {
	in, errOpen := directio.OpenFile(path, flag, perm)
	if errOpen != nil {
		return nil, errOpen
	}
	return &reader{
		mu:           &sync.Mutex{},
		fd:           in,
		buff:         nil,
		alignedBlock: directio.AlignedBlock(directIOBlockCount * directio.BlockSize),
	}, nil
}

func (r *reader) readBuff() error {
	if n, err := io.ReadFull(r.fd, r.alignedBlock); err != nil {
		r.buff = append(r.buff[r.buffOffset:], r.alignedBlock[0:n]...)
		r.buffOffset = 0
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		return err
	}
	r.buff = append(r.buff[r.buffOffset:], r.alignedBlock...)
	r.buffOffset = 0
	return nil
}

func (r *reader) copyBuff(p []byte) int {
	n := copy(p, r.buff[r.buffOffset:])
	r.buffOffset += n
	return n
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		panic(fmt.Errorf("this is programm bug, seek with negative offset is not supported by DirectIOReadSeekCloser"))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buffOffset = 0
	r.buff = nil
	r.alignedBlock = directio.AlignedBlock(directIOBlockCount * directio.BlockSize)
	if whence != io.SeekStart {
		panic(fmt.Errorf("this is programm bug, seek with whence %d currently is not supported by DirectIOReadSeekCloser",
			whence))
	}
	blocks := offset / directio.BlockSize
	if n, errSeek := r.fd.Seek(blocks*directio.BlockSize, io.SeekStart); errSeek != nil {
		return n, errSeek
	}
	nullBytes := make([]byte, offset-blocks*directio.BlockSize)
	size, errRead := r.read(nullBytes)
	if errRead != nil {
		if errors.Is(errRead, io.EOF) && int64(size) < offset {
			panic(fmt.Errorf("this is program bug, increase with seek is not supported by DirectIOReadSeekCloser"))
		}
		return int64(size), errRead
	}
	return offset, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.read(p)
}

func (r *reader) read(p []byte) (n int, err error) {
	for len(p) > len(r.buff)-r.buffOffset {
		if errRead := r.readBuff(); errRead != nil {
			if errors.Is(errRead, io.EOF) {
				if len(p) > len(r.buff)-r.buffOffset {
					n = copy(p, r.buff[r.buffOffset:])
					r.buff = nil
					return n, errRead
				}
				return r.copyBuff(p), nil
			}
			return 0, errRead
		}
	}
	return r.copyBuff(p), nil
}

func (r *reader) Close() error {
	return r.fd.Close()
}
