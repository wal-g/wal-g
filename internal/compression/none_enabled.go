package compression

import (
	"github.com/wal-g/wal-g/internal/compression/none"
)

func init() {
	Decompressors = append(Decompressors, none.Decompressor{})
	Compressors[none.AlgorithmName] = none.Compressor{}
	CompressingAlgorithms = append(CompressingAlgorithms, none.AlgorithmName)
}
