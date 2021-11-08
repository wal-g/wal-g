package splitmerge

import (
	"github.com/wal-g/tracelog"
	"io"
)

func MergeWriter(sink io.Writer, parts int, blockSize int) []io.WriteCloser {
	result := make([]io.WriteCloser, 0)
	channels := make([]chan []byte, 0)
	writeResults := make([]chan writeResult, 0)

	for i := 0; i < parts; i++ {
		channels = append(channels, make(chan []byte))
		writeResults = append(writeResults, make(chan writeResult))
		writer := newChannelWriter(channels[i], writeResults[i], blockSize)
		result = append(result, writer)
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
				writeResults[i] <- writeResult{ n: wbytes, err: err	}
				if wbytes != rbytes {
					tracelog.DebugLogger.Printf("%d / %d bytes written due to %v", wbytes, rbytes, err)
				}
				if err != nil {
					tracelog.DebugLogger.Printf("MergeWriter error: %v", err)
				}
			}

			if closed == len(channels) {
				tracelog.DebugLogger.Printf("MergeWriter: finished")
				return
			}
		}
	}()

	return result
}
