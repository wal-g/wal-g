package zstd

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

const (
	AlgorithmName = "zstd"
	FileExtension = "zst"
)

// Compressor writes zstd-compressed streams. A zero Level keeps the historical
// default (zstd.SpeedDefault), so an unconfigured Compressor behaves as before.
type Compressor struct {
	Level zstd.EncoderLevel
}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	level := compressor.Level
	if level == 0 { // level not set: preserve the previous default
		level = zstd.SpeedDefault
	}
	zw, err := zstd.NewWriter(writer,
		zstd.WithEncoderLevel(level),
	)
	if err != nil {
		panic(err)
	}

	return zw
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}

// EncoderLevelFromName resolves a WALG_ZSTD_LEVEL value ("fastest", "default",
// "better", "best") to a zstd encoder level. The match ignores case; the
// returned bool is false when the name is not recognized.
func EncoderLevelFromName(name string) (zstd.EncoderLevel, bool) {
	ok, level := zstd.EncoderLevelFromString(name)
	return level, ok
}
