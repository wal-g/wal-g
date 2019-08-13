package internal

import (
	"fmt"
	"github.com/wal-g/wal-g/internal/tracelog"
	"os"
)

func ParseDelta(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	deltaFile, err := LoadDeltaFile(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, location := range deltaFile.Locations {
		tracelog.InfoLogger.Printf("DBNode: %d, RelNode: %d, SpcNode: %d, BlockNo: %d",
			location.RelationFileNode.DBNode, location.RelationFileNode.RelNode,
			location.RelationFileNode.SpcNode, location.BlockNo)
	}
}
