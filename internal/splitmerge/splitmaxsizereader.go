package splitmerge

import (
	"github.com/wal-g/tracelog"
	"io"
)

func SplitMaxSizeReader(reader io.Reader, blockSize int, maxSize int) <-chan io.ReadCloser {
	out := make(chan io.ReadCloser)

	go func() {
		sum := 0
		bytesChannel := make(chan []byte)
		out <- NewChannelReader(bytesChannel)

		for {
			buffer := make([]byte, blockSize)
			bytes, err := io.ReadFull(reader, buffer)

			if err == io.EOF {
				break
			} else if err != nil && err != io.ErrUnexpectedEOF {
				tracelog.ErrorLogger.FatalOnError(err)
				break
			}

			if sum+bytes > maxSize {
				bytesChannel <- buffer[:maxSize-sum]
				close(bytesChannel)
				bytesChannel = make(chan []byte)
				out <- NewChannelReader(bytesChannel)
				bytesChannel <- buffer[maxSize-sum : bytes]
				sum = sum + bytes - maxSize
			} else {
				bytesChannel <- buffer[:bytes]
				sum += bytes
			}
		}
		close(bytesChannel)
		close(out)
	}()

	return out
}
