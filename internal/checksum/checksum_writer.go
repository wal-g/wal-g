package checksum

import "io"

type WriterWithChecksum struct {
	Underlying io.WriteCloser
	Calculator *Calculator
}

func CreateWriterWithChecksum(underlying io.WriteCloser, calculator *Calculator) *WriterWithChecksum {
	return &WriterWithChecksum{Underlying: underlying, Calculator: calculator}
}

func (writer *WriterWithChecksum) Write(data []byte) (n int, err error) {
	n, err = writer.Underlying.Write(data)
	writer.Calculator.AddData(data[0:n])
	return
}

func (writer *WriterWithChecksum) Close() error {
	return writer.Underlying.Close()
}
