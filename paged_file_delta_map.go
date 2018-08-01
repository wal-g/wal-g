package walg

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	RelFileSizeBound               = 1 << 30
	BlockSize                      = RelFileSizeBound / int(WalPageSize)
	DefaultSpcNode   walparser.Oid = 1663
)

var UnknownTableSpaceError = errors.New("getRelFileNodeFrom: unknown tablespace")

type PagedFileDeltaMap map[walparser.RelFileNode]*roaring.Bitmap

func NewPagedFileDeltaMap() *PagedFileDeltaMap {
	deltaMap := make(map[walparser.RelFileNode]*roaring.Bitmap)
	pagedFileDeltaMap := PagedFileDeltaMap(deltaMap)
	return &pagedFileDeltaMap
}

func (deltaMap *PagedFileDeltaMap) addToDelta(location walparser.BlockLocation) {
	_, contains := (*deltaMap)[location.RelationFileNode]
	if contains {
		(*deltaMap)[location.RelationFileNode] = roaring.BitmapOf(location.BlockNo)
	} else {
		bitmap := (*deltaMap)[location.RelationFileNode]
		bitmap.Add(location.BlockNo)
	}
}

func (deltaMap *PagedFileDeltaMap) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	relFileNode, err := getRelFileNodeFrom(filePath)
	if err != nil {
		return nil, err
	}
	bitmap := (*deltaMap)[*relFileNode].Clone()
	rangeNum, err := getRangeNumberFrom(filePath)
	if err != nil {
		return nil, err
	}
	return selectRange(bitmap, rangeNum), nil
}

func selectRange(bitmap *roaring.Bitmap, rangeNum int) *roaring.Bitmap {
	rangeBitmap := roaring.New()
	rangeBitmap.AddRange(uint64(rangeNum*BlockSize), uint64((rangeNum+1)*BlockSize))
	bitmap.And(rangeBitmap)
	shiftedBitmap := roaring.New()
	it := bitmap.Iterator()
	for it.HasNext() {
		shiftedBitmap.Add(it.Next() - uint32(rangeNum*BlockSize))
	}
	return shiftedBitmap
}

func getRangeNumberFrom(filePath string) (int, error) {
	fileName := path.Base(filePath)
	match := pagedFilenameRegexp.FindStringSubmatch(fileName)
	if match[2] == "" {
		return 0, nil
	}
	return strconv.Atoi(match[2][1:])
}

func getRelFileNodeFrom(filePath string) (*walparser.RelFileNode, error) {
	folderPath, name := path.Split(filePath)
	folderPathParts := strings.Split(sanitizePath(folderPath), string(os.PathSeparator))
	match := pagedFilenameRegexp.FindStringSubmatch(name)
	relNode, err := strconv.Atoi(match[1])
	if err != nil {
		return nil, err
	}
	dbNode, err := strconv.Atoi(folderPathParts[len(folderPathParts)-1])
	if err != nil {
		return nil, err
	}
	if strings.Contains(filePath, DefaultTablespace) { // base
		return &walparser.RelFileNode{SpcNode: DefaultSpcNode, DBNode: walparser.Oid(dbNode), RelNode: walparser.Oid(relNode)}, nil
	} else if strings.Contains(filePath, NonDefaultTablespace) { // pg_tblspc
		spcNode, err := strconv.Atoi(folderPathParts[len(folderPathParts)-3])
		if err != nil {
			return nil, err
		}
		return &walparser.RelFileNode{SpcNode: walparser.Oid(spcNode), DBNode: walparser.Oid(dbNode), RelNode: walparser.Oid(relNode)}, nil
	} else {
		return nil, UnknownTableSpaceError
	}
}
