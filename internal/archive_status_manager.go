package internal

import (
	"github.com/fsnotify/fsnotify"
	"github.com/wal-g/wal-g/internal/tracelog"
	"os"
	"path/filepath"
	"strings"
)

type ArchiveStatusManager struct {
	archiveInfoFolder   *DiskDataFolder
	archiveStatusFolder *DiskDataFolder

	wather *fsnotify.Watcher
}

func (manager *ArchiveStatusManager) removeStatusFile(filename string) error {
	_, name := filepath.Split(filename)
	extension := filepath.Ext(name)

	absInfoPath := filepath.Join(manager.archiveInfoFolder.path, strings.TrimSuffix(name, extension))

	_ = manager.wather.Remove(filename)
	err := os.Remove(absInfoPath)
	return err
}

func (manager *ArchiveStatusManager) FileExist(filename string) bool {
	_, err := os.Stat(filepath.Join(manager.archiveInfoFolder.path, filename))

	if !os.IsNotExist(err) && nil != err{
		tracelog.ErrorLogger.Printf("Error of archiveStatusManager %v", err)
	}

	return !os.IsNotExist(err)
}

func (manager *ArchiveStatusManager) createStatusFile(filename string) error {
	absInfoPath := filepath.Join(manager.archiveInfoFolder.path, filename)
	absStatPath := filepath.Join(manager.archiveStatusFolder.path, filename+".ready")

	_, err := os.Create(absInfoPath)
	err = manager.wather.Add(absStatPath)
	return err
}

func NewArchiveStatusManager(archiveInfoFolder, archiveStatusFolder *DiskDataFolder) *ArchiveStatusManager {

	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		tracelog.ErrorLogger.Printf("Unable to configure Archive manager due to error %v\n", err)
		return nil
	}

	manager := &ArchiveStatusManager{archiveInfoFolder, archiveStatusFolder, watcher}

	go func() {
		for {
			select {
			// watch for events
			case event := <-watcher.Events:
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					_ = manager.removeStatusFile(event.Name)
				}

			// watch for errors
			case err := <-watcher.Errors:
				if err != nil {
					tracelog.ErrorLogger.Print("ERROR", err)
				}
			}
		}
	}()

	return manager
}
