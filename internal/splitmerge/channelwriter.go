package splitmerge

import (
	"io"
)

type writeResult struct {
	n   int
	err error
}

// channelWriter provider io.WriteCloser interface on top of `ch` chan []byte
// i.e. it sends all data written by Write(data) to `ch` channel and waits for response: bytes written & errors
type channelWriter struct {
	ch       chan<- []byte
	resultCh <-chan writeResult
}

var _ io.WriteCloser = &channelWriter{}

func newChannelWriter(ch chan<- []byte, resultCh <-chan writeResult) io.WriteCloser {
	return &channelWriter{
		ch:       ch,
		resultCh: resultCh,
	}
}

func (cw *channelWriter) Write(data []byte) (int, error) {
	cw.ch <- data
	wr, ok := <-cw.resultCh
	if !ok {
		return 0, io.ErrShortWrite
	}
	return wr.n, wr.err
}

func (cw *channelWriter) Close() error {
	close(cw.ch)
	return nil
}
