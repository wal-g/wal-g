package checksum

import "io"

type ReaderWithChecksum struct {
	Underlying io.Reader
	Calculator *Calculator
}

func CreateReaderWithChecksum(underlying io.Reader, calculator *Calculator) *ReaderWithChecksum {
	return &ReaderWithChecksum{Underlying: underlying, Calculator: calculator}
}

func (reader *ReaderWithChecksum) Read(data []byte) (n int, err error) {
	n, err = reader.Underlying.Read(data)
	reader.Calculator.AddData(data[0:n])
	return
}
