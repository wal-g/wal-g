package innodb

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"
)

var ErrSpaceIDNotFound = errors.New("SpaceID not found")

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

	err := filepath.WalkDir(c.dataDir, func(path string, info fs.DirEntry, walkErr error) error {
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
	file, err := os.OpenFile(filePath, os.O_RDONLY|syscall.O_NOFOLLOW, 0) // FIXME: test performance with O_SYNC
	tracelog.ErrorLogger.FatalfOnError("Cannot open file: %v", err)

	reader := NewPageReader(file)
	if reader == nil {
		return fmt.Errorf("canot read innodb file %v", filePath)
	}
	if !strings.HasPrefix(filePath, c.dataDir) {
		tracelog.ErrorLogger.Fatalf("File %v is out of data dir %v", filePath, c.dataDir)
	}
	fileName := filePath[len(c.dataDir):]
	c.collected[reader.SpaceID] = strings.TrimPrefix(fileName, "/")
	return nil
}

func (c *spaceIDCollectorImpl) GetFileForSpaceID(spaceID SpaceID) (string, error) {
	if c.collected == nil {
		return "", fmt.Errorf("spaceIDCollectorImpl not initialized")
	}
	result, ok := c.collected[spaceID]
	if ok {
		return result, nil
	}
	return "", fmt.Errorf("file for SpaceID %v not found: %w", spaceID, ErrSpaceIDNotFound)
}
