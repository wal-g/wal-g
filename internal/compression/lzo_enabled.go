// +build lzo

package compression

import "github.com/wal-g/wal-g/internal/compression/lzo"

func init() {
	Decompressors = append(Decompressors, lzo.Decompressor{})
}
