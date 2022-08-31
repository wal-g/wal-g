//go:build !windows
// +build !windows

package compression

import (
	"github.com/wal-g/wal-g/internal/compression/gzip"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"github.com/wal-g/wal-g/internal/compression/zstd"
)

var CompressingAlgorithms = []string{lz4.AlgorithmName, lzma.AlgorithmName, zstd.AlgorithmName}

var Compressors = map[string]Compressor{
	lz4.AlgorithmName:  lz4.Compressor{},
	lzma.AlgorithmName: lzma.Compressor{},
	zstd.AlgorithmName: zstd.Compressor{},
}

var Decompressors = []Decompressor{
	lz4.Decompressor{},
	lzma.Decompressor{},
	zstd.Decompressor{},
	gzip.Decompressor{},
}
