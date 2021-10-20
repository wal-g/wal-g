package storage

import (
	"github.com/wal-g/tracelog"
	"io"
)

type ChannelWriter struct {
	ch        chan []byte
	block     []byte
	offset    int
	blockSize int
}

var _ io.WriteCloser = &ChannelWriter{}

func (mw *ChannelWriter) Write(data []byte) (int, error) {
	dataOffset := 0

	for {
		bytes := copy(mw.block[mw.offset:], data[dataOffset:])
		mw.offset += bytes
		dataOffset += bytes

		if mw.offset == len(mw.block) {
			//tracelog.DebugLogger.Printf("ChannelWriter. WRITE %d bytes", len(mw.block))

			mw.ch <- mw.block
			mw.block = make([]byte, mw.blockSize)
			mw.offset = 0
		}
		if dataOffset == len(data) {
			return len(data), nil

		}
	}

}

func (mw *ChannelWriter) Close() error {
	if mw.offset < len(mw.block) && mw.offset > 0 {
		// tracelog.DebugLogger.Printf("ChannelWriter. WRITE %d bytes [on close]", mw.offset)
		mw.ch <- mw.block[:mw.offset]
	}
	close(mw.ch)
	return nil
}

func NewChannelWriter(ch chan []byte, blockSize int) io.WriteCloser {
	return &ChannelWriter{
		ch:        ch,
		block:     make([]byte, blockSize),
		blockSize: blockSize,
	}
}

func MergeWriter(sink io.Writer, parts int, blockSize int) ([]io.WriteCloser, <-chan error) {
	result := make([]io.WriteCloser, 0)
	channels := make([]chan []byte, 0)
	done := make(chan error)

	for i := 0; i < parts; i++ {
		channels = append(channels, make(chan []byte, 10)) // buffered channel
		result = append(result, NewChannelWriter(channels[i], blockSize))
	}

	// start MergeWriter:
	go func() {
		for {
			closed := 0
			for i, ch := range channels {
				block, ok := <-ch
				if !ok {
					tracelog.DebugLogger.Printf("MergeWriter. #%d closed", i)
					closed++
					continue
				}
				rbytes := len(block)
				wbytes, err := sink.Write(block)
				if wbytes != rbytes {
					tracelog.ErrorLogger.Printf("%d / %d bytes written due to %v", wbytes, rbytes, err)
				}
				if err != nil {
					tracelog.ErrorLogger.Printf("MergeWriter error: %v", err)
					done <- err
					close(done)
					return
				}
			}

			if closed == len(channels) {
				tracelog.DebugLogger.Printf("MergeWriter: success")
				close(done)
				return
			}
		}
	}()

	return result, done
}
