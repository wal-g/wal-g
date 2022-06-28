package postgres

import (
	"fmt"

	"github.com/wal-g/wal-g/internal/walparser"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	AoStoragePath = "aosegments"
	AoSegSuffix   = "_aoseg"
)

func makeAoFileStorageKey(relNameMd5 string, modCount int64, location *walparser.BlockLocation) string {
	return fmt.Sprintf("%d_%d_%s_%d_%d_%d%s",
		location.RelationFileNode.SpcNode, location.RelationFileNode.DBNode,
		relNameMd5,
		location.RelationFileNode.RelNode, location.BlockNo,
		modCount, AoSegSuffix)
}

func LoadStorageAoFiles(baseBackupsFolder storage.Folder) (map[string]struct{}, error) {
	aoObjects, _, err := baseBackupsFolder.GetSubFolder(AoStoragePath).ListFolder()
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{})
	for _, obj := range aoObjects {
		result[obj.GetName()] = struct{}{}
	}

	return result, nil
}
