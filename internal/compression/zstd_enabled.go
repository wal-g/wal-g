//go:build !windows && !(arm64 && darwin)
// +build !windows
// +build !arm64 !darwin

package compression

import (
	"github.com/wal-g/wal-g/internal/compression/zstd"
)

func init() {
	Decompressors = append(Decompressors, zstd.Decompressor{})
	Compressors[zstd.AlgorithmName] = zstd.Compressor{}
	CompressingAlgorithms = append(CompressingAlgorithms, zstd.AlgorithmName)
}
