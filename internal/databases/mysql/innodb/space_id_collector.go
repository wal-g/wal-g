package innodb

import (
	"fmt"
	"github.com/wal-g/tracelog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type SpaceIDCollector interface {
	Collect() error
	GetFileForSpaceID(spaceID SpaceID) (string, error)
}

type spaceIDCollectorImpl struct {
	dataDir   string
	collected map[SpaceID]string
}

var _ SpaceIDCollector = &spaceIDCollectorImpl{}

func NewSpaceIDCollector(dataDir string) SpaceIDCollector {
	return &spaceIDCollectorImpl{dataDir: dataDir}
}

func (c *spaceIDCollectorImpl) Collect() error {
	c.collected = make(map[SpaceID]string)

	err := filepath.Walk(c.dataDir, func(path string, info os.FileInfo, walkErr error) error {
		tracelog.ErrorLogger.FatalfOnError("Error encountered during datadir traverse", walkErr)
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".ibd") {
			err := c.collect(path)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (c *spaceIDCollectorImpl) collect(filePath string) error {
	// read first FPS page (always first page in the file)
	file, err := os.OpenFile(filePath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)

	reader := NewPageReader(file)
	if reader != nil {
		// FIXME: datadir shouldn't be included!
		c.collected[reader.SpaceID] = filePath
	}
	return nil
}

func (c *spaceIDCollectorImpl) GetFileForSpaceID(spaceID SpaceID) (string, error) {
	if c.collected == nil {
		return "", fmt.Errorf("spaceIDCollectorImpl not initialised")
	}
	result, ok := c.collected[spaceID]
	if ok {
		return result, nil
	} else {
		return "", fmt.Errorf("SpaceID not found")
	}

}
