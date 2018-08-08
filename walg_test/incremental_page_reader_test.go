package walg_test

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"testing"
)

func TestDeltaBitmapInitialize(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		FileSize: int64(walg.WalPageSize * 5),
		Blocks:   make([]uint32, 0),
	}
	deltaBitmap := roaring.BitmapOf(0, 2, 3, 12, 14)
	pageReader.DeltaBitmapInitialize(deltaBitmap)
	assert.Equal(t, pageReader.Blocks, []uint32{0, 2, 3})
}
