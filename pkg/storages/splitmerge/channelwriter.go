package splitmerge

import "io"

type writeResult struct {
	n   int
	err error
}

// channelWriter provider io.WriteCloser interface on top of `ch` chan []byte
// i.e. it sends all data written by Write(data) to `ch` channel and waits for response: bytes written + errors
type channelWriter struct {
	// users must not retain data slices after sending response to resultCh
	ch        chan<- []byte
	resultCh  <-chan writeResult
	block     []byte
	offset    int
	blockSize int
}

var _ io.WriteCloser = &channelWriter{}

func newChannelWriter(ch chan<- []byte, resultCh <-chan writeResult, blockSize int) io.WriteCloser {
	return &channelWriter{
		ch:        ch,
		resultCh:  resultCh,
		block:     make([]byte, blockSize),
		blockSize: blockSize,
	}
}

func (cw *channelWriter) Write(data []byte) (int, error) {
	dataOffset := 0

	for {
		bytes := copy(cw.block[cw.offset:], data[dataOffset:])
		cw.offset += bytes
		dataOffset += bytes

		if cw.offset == len(cw.block) {
			cw.ch <- cw.block
			wr := <-cw.resultCh
			if wr.err != nil {
				cw.offset = 0
				return dataOffset - cw.blockSize + wr.n, wr.err
			}
			cw.block = make([]byte, cw.blockSize)  // FIXME: don't allocate memory: cw.block = cw.block[:0]
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
		wr := <-cw.resultCh
		if wr.err != nil {
			return wr.err
		}
	}
	close(cw.ch)
	return nil
}