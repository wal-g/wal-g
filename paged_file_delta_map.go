package walg

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"os"
	"path"
	"strconv"
	"strings"
	"fmt"
)

const (
	RelFileSizeBound               = 1 << 30
	BlocksInRelFile                = RelFileSizeBound / int(DatabasePageSize)
	DefaultSpcNode   walparser.Oid = 1663
)

type NoBitmapFoundError struct {
	error
}

func NewNoBitmapFoundError() NoBitmapFoundError {
	return NoBitmapFoundError{errors.New("GetDeltaBitmapFor: no bitmap found")}
}

func (err NoBitmapFoundError) Error() string {
	return fmt.Sprintf("%+v", err.error)
}

type UnknownTableSpaceError struct {
	error
}

func NewUnknownTableSpaceError() UnknownTableSpaceError {
	return UnknownTableSpaceError{errors.New("GetRelFileNodeFrom: unknown tablespace")}
}

func (err UnknownTableSpaceError) Error() string {
	return fmt.Sprintf("%+v", err.error)
}

type PagedFileDeltaMap map[walparser.RelFileNode]*roaring.Bitmap

func NewPagedFileDeltaMap() PagedFileDeltaMap {
	deltaMap := make(map[walparser.RelFileNode]*roaring.Bitmap)
	pagedFileDeltaMap := PagedFileDeltaMap(deltaMap)
	return pagedFileDeltaMap
}

func (deltaMap *PagedFileDeltaMap) AddToDelta(location walparser.BlockLocation) {
	_, contains := (*deltaMap)[location.RelationFileNode]
	if !contains {
		(*deltaMap)[location.RelationFileNode] = roaring.BitmapOf(location.BlockNo)
	} else {
		bitmap := (*deltaMap)[location.RelationFileNode]
		bitmap.Add(location.BlockNo)
	}
}

// TODO : unit test no bitmap found
func (deltaMap *PagedFileDeltaMap) GetDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		return nil, err
	}
	_, ok := (*deltaMap)[*relFileNode]
	if !ok {
		return nil, NewNoBitmapFoundError()
	}
	bitmap := (*deltaMap)[*relFileNode].Clone()
	relFileId, err := GetRelFileIdFrom(filePath)
	if err != nil {
		return nil, err
	}
	return SelectRelFileBlocks(bitmap, relFileId), nil
}

func SelectRelFileBlocks(bitmap *roaring.Bitmap, relFileId int) *roaring.Bitmap {
	relFileBitmap := roaring.New()
	relFileBitmap.AddRange(uint64(relFileId*BlocksInRelFile), uint64((relFileId+1)*BlocksInRelFile))
	bitmap.And(relFileBitmap)
	shiftedBitmap := roaring.New()
	it := bitmap.Iterator()
	for it.HasNext() {
		shiftedBitmap.Add(it.Next() - uint32(relFileId*BlocksInRelFile))
	}
	return shiftedBitmap
}

func GetRelFileIdFrom(filePath string) (int, error) {
	filename := path.Base(filePath)
	match := pagedFilenameRegexp.FindStringSubmatch(filename)
	if match[2] == "" {
		return 0, nil
	}
	return strconv.Atoi(match[2][1:])
}

func GetRelFileNodeFrom(filePath string) (*walparser.RelFileNode, error) {
	folderPath, name := path.Split(filePath)
	folderPathParts := strings.Split(strings.TrimSuffix(folderPath, "/"), string(os.PathSeparator))
	match := pagedFilenameRegexp.FindStringSubmatch(name)
	relNode, err := strconv.Atoi(match[1])
	if err != nil {
		return nil, errors.Wrapf(err, "GetRelFileNodeFrom: can't get relNode from: '%s'", filePath)
	}
	dbNode, err := strconv.Atoi(folderPathParts[len(folderPathParts)-1])
	if err != nil {
		return nil, errors.Wrapf(err, "GetRelFileNodeFrom: can't get dbNode from: '%s'", filePath)
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
		return nil, NewUnknownTableSpaceError()
	}
}
