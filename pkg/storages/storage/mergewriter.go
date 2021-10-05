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
				//tracelog.DebugLogger.Printf("MergeWriter. #%d read: %d bytes", i, len(block))
				if !ok {
					tracelog.DebugLogger.Printf("MergeWriter. #%d closed", i)
					closed++
					continue
				}
				wbytes, err := sink.Write(block) // FIXME: handle return values!
				if wbytes != len(block) {
					tracelog.ErrorLogger.Fatalf("%d / %d bytes written", wbytes, len(block))
				}
				if err != nil {
					tracelog.ErrorLogger.Printf("%v", err)
					done <- err
					return
				}
				//tracelog.DebugLogger.Printf("MergeWriter. bytes written: %d", wbytes)
			}

			if closed == len(channels) {
				tracelog.DebugLogger.Printf("MergeWriter: success")
				done <- nil
				return
			}
		}
	}()

	return result, done
}
