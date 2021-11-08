package splitmerge

import (
	"github.com/wal-g/tracelog"
	"io"
)

// MergeWriter returns list of WriteCloser-s
// Then it reads data from each of n=`parts` WriteClosers in blocks of `blockSize` and writes data to `sink` writer.
func MergeWriter(sink io.Writer, parts int, blockSize int) []io.WriteCloser {
	result := make([]io.WriteCloser, 0)
	channels := make([]chan []byte, 0)
	writeResults := make([]chan writeResult, 0)

	for i := 0; i < parts; i++ {
		channels = append(channels, make(chan []byte))
		writeResults = append(writeResults, make(chan writeResult))
		cw := newChannelWriter(channels[i], writeResults[i])
		fbsw := newFixedBlockSizeWriter(cw, blockSize)
		result = append(result, fbsw)
	}

	// start MergeWriter:
	go func() {
		defer (func() {
			for _, wrch := range writeResults {
				close(wrch)
			}
		})()

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
				writeResults[i] <- writeResult{n: wbytes, err: err}
				if wbytes != rbytes {
					tracelog.DebugLogger.Printf("%d / %d bytes written due to %v", wbytes, rbytes, err)
				}
				if err != nil {
					tracelog.ErrorLogger.Printf("MergeWriter error: %v", err)
					return
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
