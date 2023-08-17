package splitmerge

import (
	"context"
	"io"

	"github.com/wal-g/tracelog"
)

func SplitReader(ctx context.Context, reader io.Reader, parts int, blockSize int) []io.Reader {
	result := make([]io.Reader, 0)
	channels := make([]chan []byte, 0)

	for i := 0; i < parts; i++ {
		channels = append(channels, make(chan []byte))
		result = append(result, NewChannelReader(channels[i]))
	}

	// start SplitReader:
	go func() {
		idx := 0
		for {
			block := make([]byte, blockSize)
			bytes, err := io.ReadFull(reader, block)
			if bytes != 0 {
				select {
				case channels[idx] <- block[0:bytes]:
				case <-ctx.Done():
					for i := 0; i < parts; i++ {
						close(channels[i])
					}
					tracelog.InfoLogger.Println("SplitReader closed until the end of the work")
					return
				}

				if bytes != blockSize {
					tracelog.InfoLogger.Printf("SplitReader. #%d send: %d / %d bytes", idx, bytes, blockSize)
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
