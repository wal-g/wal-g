package internal

import (
	"io/ioutil"
	"os"

	"github.com/wal-g/wal-g/internal/tracelog"
)

// FileSystemCleaner actually performs it's functions on file system
type FileSystemCleaner struct{}

// TODO : unit tests
// GetFiles of a directory
func (cleaner FileSystemCleaner) GetFiles(directory string) (files []string, err error) {
	fileInfos, err := ioutil.ReadDir(directory)
	if err != nil {
		return
	}
	files = make([]string, 0)
	for i := 0; i < len(fileInfos); i++ {
		if fileInfos[i].IsDir() {
			continue
		}
		files = append(files, fileInfos[i].Name())
	}
	return
}

// Remove file
func (cleaner FileSystemCleaner) Remove(file string) {
	err := os.Remove(file)
	if err != nil {
		tracelog.WarningLogger.Printf("Tried to remove file: '%s', but got error: '%v'\n", file, err)
	}
}
