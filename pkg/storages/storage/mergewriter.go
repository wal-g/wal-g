package storage

import (
	"github.com/wal-g/tracelog"
	"io"
)

type channelWriter struct {
	ch        chan []byte
	block     []byte
	offset    int
	blockSize int
}

var _ io.WriteCloser = &channelWriter{}

func (cw *channelWriter) Write(data []byte) (int, error) {
	dataOffset := 0

	for {
		bytes := copy(cw.block[cw.offset:], data[dataOffset:])
		cw.offset += bytes
		dataOffset += bytes

		if cw.offset == len(cw.block) {
			cw.ch <- cw.block
			cw.block = make([]byte, cw.blockSize)
			cw.offset = 0
		}
		if dataOffset == len(data) {
			return len(data), nil

		}
	}
}

func (cw *channelWriter) Close() error {
	if cw.offset < len(cw.block) && cw.offset > 0 {
		cw.ch <- cw.block[:cw.offset]
	}
	close(cw.ch)
	return nil
}

func NewChannelWriter(ch chan []byte, blockSize int) io.WriteCloser {
	return &channelWriter{
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
		channels = append(channels, make(chan []byte, 0))
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
