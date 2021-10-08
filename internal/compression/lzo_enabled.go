//go:build lzo && !windows
// +build lzo,!windows

package compression

import "github.com/wal-g/wal-g/internal/compression/lzo"

func init() {
	Decompressors = append(Decompressors, lzo.Decompressor{})
}
