package innodb

import (
	"errors"
	"fmt"
	"github.com/wal-g/wal-g/utility"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"
)

var ErrSpaceIDNotFound = errors.New("SpaceID not found")

type SpaceIDCollector interface {
	// GetFileForSpaceID locates InnoDB file (path relative to dataDir) for requested SpaceID
	GetFileForSpaceID(spaceID SpaceID) (string, error)
	// CheckFileForSpaceID tests filePath (path relative to dataDir) to be InnoDB file whether it has requested SpaceID
	CheckFileForSpaceID(spaceID SpaceID, filePath string) error
}

type spaceIDCollectorImpl struct {
	dataDir   string
	collected map[SpaceID]string
}

var _ SpaceIDCollector = &spaceIDCollectorImpl{}

func NewSpaceIDCollector(dataDir string) (SpaceIDCollector, error) {
	result := &spaceIDCollectorImpl{dataDir: dataDir}
	result.collected = make(map[SpaceID]string)

	// https://github.com/percona/percona-xtrabackup/blob/percona-xtrabackup-8.0.35-30/
	// storage/innobase/xtrabackup/src/xtrabackup.cc#L5321-L5567

	err := filepath.WalkDir(dataDir, func(path string, info fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("error encountered during dataDir traverse %v: %w", path, walkErr)
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".ibd") {
			_, err := result.collect(path)
			if err != nil && !errors.Is(err, ErrSpaceIDNotFound) {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	tracelog.DebugLogger.Printf("SpaceIDCollector for dir %v collected %v", dataDir, result.collected)

	return result, nil
}

func (c *spaceIDCollectorImpl) collect(filePath string) (SpaceID, error) {
	// read first FPS page (always first page in the file)
	file, err := os.OpenFile(filePath, os.O_RDONLY|syscall.O_NOFOLLOW, 0) // FIXME: test performance with O_SYNC
	if err != nil {
		tracelog.DebugLogger.Printf("error opening file %v: %v", filePath, err)
		return SpaceIDUnknown, ErrSpaceIDNotFound
	}

	reader, err := NewPageReader(file)
	if err != nil {
		tracelog.InfoLogger.Printf("cannot collect spaceID from file %v: %v", filePath, err)
		return SpaceIDUnknown, ErrSpaceIDNotFound
	}
	defer utility.LoggedClose(reader, "")

	// FIXME: use os.Root [go 1.24] https://github.com/golang/go/issues/67002
	if !strings.HasPrefix(filePath, c.dataDir) {
		tracelog.ErrorLogger.Fatalf("File %v is out of data dir %v", filePath, c.dataDir)
	}
	if prevPath, ok := c.collected[reader.SpaceID]; ok {
		tracelog.ErrorLogger.Fatalf("duplicate SpaceID %v found '%v' and '%v'", reader.SpaceID, prevPath, filePath)
	}
	fileName := filePath[len(c.dataDir):]
	c.collected[reader.SpaceID] = strings.TrimPrefix(fileName, "/")
	return reader.SpaceID, nil
}

func (c *spaceIDCollectorImpl) GetFileForSpaceID(spaceID SpaceID) (string, error) {
	result, ok := c.collected[spaceID]
	if ok {
		return result, nil
	}
	return "", fmt.Errorf("file for SpaceID %v not found: %w", spaceID, ErrSpaceIDNotFound)
}

func (c *spaceIDCollectorImpl) CheckFileForSpaceID(spaceID SpaceID, filePath string) error {
	// MySQL can store InnoDB files in multiple places, with different file extensions
	// we may not be aware of these files... so check suggested pair spaceID + filePath
	actualSpaceID, err := c.collect(path.Join(c.dataDir, filePath))
	if err != nil {
		return err
	}
	if actualSpaceID != spaceID {
		return ErrSpaceIDNotFound
	}
	return nil
}
