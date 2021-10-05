package storage

import (
	"github.com/wal-g/tracelog"
	"io"
)

func SplitReader(reader io.Reader, parts int, blockSize int) []io.Reader {
	result := make([]io.Reader, 0)
	channels := make([]chan []byte, 0)

	for i := 0; i < parts; i++ {
		channels = append(channels, make(chan []byte, 10)) // buffered channel
		result = append(result, NewChannelReader(channels[i]))
	}

	// start SplitReader:
	go func() {
		idx := 0
		for {
			block := make([]byte, blockSize)
			bytes, err := io.ReadFull(reader, block)
			if bytes != 0 {
				channels[idx] <- block[0:bytes]
				if bytes != blockSize {
					tracelog.ErrorLogger.Printf("SplitReader. #%d send: %d / %d bytes", idx, bytes, blockSize)
				}
			}
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				for _, ch := range channels {
					close(ch)
				}
				return
			}
			idx = (idx + 1) % len(channels)
		}
	}()

	return result
}
