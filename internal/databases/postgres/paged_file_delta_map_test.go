package postgres_test

import (
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/walparser"
)

func TestGetRelFileIdFrom_ZeroId(t *testing.T) {
	relFileId, err := postgres.GetRelFileIDFrom("~/DemoDb/base/16384/2668")
	assert.NoError(t, err)
	assert.Equal(t, 0, relFileId)
}

func TestGetRelFileIdFrom_NonZeroId(t *testing.T) {
	relFileId, err := postgres.GetRelFileIDFrom("~/DemoDb/base/16384/2668.3")
	assert.NoError(t, err)
	assert.Equal(t, 3, relFileId)
}

func TestGetRelFileNodeFrom_DefaultTableSpace(t *testing.T) {
	relFileNode, err := postgres.GetRelFileNodeFrom("~/DemoDb/base/123/100500")
	assert.NoError(t, err)
	assert.Equal(t, walparser.RelFileNode{SpcNode: postgres.DefaultSpcNode, DBNode: 123, RelNode: 100500}, *relFileNode)
}

func TestGetRelFileNodeFrom_IncorrectDefaultTableSpace_v1(t *testing.T) {
	_, err := postgres.GetRelFileNodeFrom("~/DemoDb/base.old/123/100500")
	assert.Error(t, err)
}

func TestGetRelFileNodeFrom_IncorrectDefaultTableSpace_v2(t *testing.T) {
	_, err := postgres.GetRelFileNodeFrom("~/DemoDb/base/some_garbage/123/100500")
	assert.Error(t, err)
}

func TestGetRelFileNodeFrom_IncorrectDefaultTableSpace_v3(t *testing.T) {
	_, err := postgres.GetRelFileNodeFrom("~/DemoDb/garbage/123/100500")
	assert.Error(t, err)
}

func TestGetRelFileNodeFrom_NonDefaultTableSpace(t *testing.T) {
	relFileNode, err := postgres.GetRelFileNodeFrom("~/DemoDb/pg_tblspc/16709/PG_9.3_201306121/16499/19401")
	assert.NoError(t, err)
	assert.Equal(t, walparser.RelFileNode{SpcNode: 16709, DBNode: 16499, RelNode: 19401}, *relFileNode)
}

func TestSelectRelFileBlocks(t *testing.T) {
	bitmap := roaring.BitmapOf(
		1, 2, 3,
		uint32(postgres.BlocksInRelFile+134), uint32(postgres.BlocksInRelFile+23),
		uint32(postgres.BlocksInRelFile*4+123), uint32(postgres.BlocksInRelFile*4+932),
		uint32(postgres.BlocksInRelFile*11+21), uint32(postgres.BlocksInRelFile*11+32),
	)
	selected := postgres.SelectRelFileBlocks(bitmap, 4)
	assert.Equal(t, []uint32{123, 932}, selected.ToArray())
}

func TestAddToDelta(t *testing.T) {
	deltaMap := postgres.NewPagedFileDeltaMap()
	location := *walparser.NewBlockLocation(1, 2, 3, 4)
	deltaMap.AddLocationToDelta(location)
	assert.Equal(t, []uint32{location.BlockNo}, deltaMap[location.RelationFileNode].ToArray())
	deltaMap.AddLocationToDelta(location)
	assert.Equal(t, []uint32{location.BlockNo}, deltaMap[location.RelationFileNode].ToArray())
}

func TestGetDeltaBitmapFor(t *testing.T) {
	blocks := []uint32{
		1, 2, 3,
		uint32(postgres.BlocksInRelFile + 134), uint32(postgres.BlocksInRelFile + 23),
		uint32(postgres.BlocksInRelFile*4 + 123), uint32(postgres.BlocksInRelFile*4 + 932),
		uint32(postgres.BlocksInRelFile*11 + 21), uint32(postgres.BlocksInRelFile*11 + 32),
	}
	testingRelNode := walparser.RelFileNode{
		SpcNode: postgres.DefaultSpcNode,
		DBNode:  1,
		RelNode: 2,
	}
	otherLocations := []walparser.BlockLocation{
		*walparser.NewBlockLocation(1, 2, 3, 10),
		*walparser.NewBlockLocation(1, 2, 3, 13),
		*walparser.NewBlockLocation(postgres.DefaultSpcNode, 1, 1, 123),
	}
	deltaMap := postgres.NewPagedFileDeltaMap()
	for _, block := range blocks {
		deltaMap.AddLocationToDelta(walparser.BlockLocation{RelationFileNode: testingRelNode, BlockNo: block})
	}
	deltaMap.AddLocationsToDelta(otherLocations)

	bitmap, err := deltaMap.GetDeltaBitmapFor("~/DemoDb/base/1/2.1")
	assert.NoError(t, err)
	assert.Equal(t, []uint32{23, 134}, bitmap.ToArray())
}
