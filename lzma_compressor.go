package walg

import "io"

type LzmaCompressor struct{}

func (compressor LzmaCompressor) NewWriter(writer io.Writer) ReaderFromWriteCloser {
	lzmaWriter, err := NewLzmaReaderFromWriter(writer)
	if err != nil {
		panic(err.Error())
	}
	return lzmaWriter
}

func (compressor LzmaCompressor) FileExtension() string {
	return LzmaFileExtension
}
