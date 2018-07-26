package walg

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"
)

// HandleWALPrefetch is invoked by wal-fetch command to speed up database restoration
func HandleWALPrefetch(pre *S3Folder, walFileName string, location string) {
	var fileName = walFileName
	var err error
	location = path.Dir(location)
	wg := &sync.WaitGroup{}
	for i := 0; i < getMaxDownloadConcurrency(8); i++ {
		fileName, err = GetNextWALFileName(fileName)
		if err != nil {
			log.Println("WAL-prefetch failed: ", err, " file: ", fileName)
		}
		wg.Add(1)
		go prefetchFile(location, pre, fileName, wg)
		time.Sleep(10 * time.Millisecond) // ramp up in order
	}

	go CleanupPrefetchDirectories(walFileName, location, FileSystemCleaner{})

	wg.Wait()
}

func prefetchFile(location string, pre *S3Folder, walFileName string, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Prefetch unsuccessful ", walFileName, r)
		}
		wg.Done()
	}()

	_, runningLocation, oldPath, newPath := GetPrefetchLocations(location, walFileName)
	_, errO := os.Stat(oldPath)
	_, errN := os.Stat(newPath)

	if (errO == nil || !os.IsNotExist(errO)) || (errN == nil || !os.IsNotExist(errN)) {
		// Seems someone is doing something about this file
		return
	}

	log.Println("WAL-prefetch file: ", walFileName)
	os.MkdirAll(runningLocation, 0755)

	DownloadAndDecompressWALFile(pre, walFileName, oldPath)

	_, errO = os.Stat(oldPath)
	_, errN = os.Stat(newPath)
	if errO == nil && os.IsNotExist(errN) {
		os.Rename(oldPath, newPath)
	} else {
		os.Remove(oldPath) // error is ignored
	}
}

func GetPrefetchLocations(location string, walFileName string) (prefetchLocation string, runningLocation string, runningFile string, fetchedFile string) {
	prefetchLocation = path.Join(location, ".wal-g", "prefetch")
	runningLocation = path.Join(prefetchLocation, "running")
	oldPath := path.Join(runningLocation, walFileName)
	newPath := path.Join(prefetchLocation, walFileName)
	return prefetchLocation, runningLocation, oldPath, newPath
}

func forkPrefetch(walFileName string, location string) {
	if strings.Contains(walFileName, "history") ||
		strings.Contains(walFileName, "partial") ||
		getMaxDownloadConcurrency(16) == 1 {
		return // There will be nothing ot prefetch anyway
	}
	cmd := exec.Command(os.Args[0], "wal-prefetch", walFileName, location)
	cmd.Env = os.Environ()
	err := cmd.Start()

	if err != nil {
		log.Println("WAL-prefetch failed: ", err)
	}
}

// Cleaner interface serves to separate file system logic from prefetch clean logic to make it testable
type Cleaner interface {
	GetFiles(directory string) ([]string, error)
	Remove(file string)
}

// FileSystemCleaner actually performs it's functions on file system
type FileSystemCleaner struct{}

// GetFiles of a directory
func (c FileSystemCleaner) GetFiles(directory string) (files []string, err error) {
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
func (c FileSystemCleaner) Remove(file string) {
	os.Remove(file)
}

func CleanupPrefetchDirectories(walFileName string, location string, cleaner Cleaner) {
	timelineId, logSegNo, err := ParseWALFileName(walFileName)
	if err != nil {
		log.Println("WAL-prefetch cleanup failed: ", err, " file: ", walFileName)
		return
	}
	prefetchLocation, runningLocation, _, _ := GetPrefetchLocations(location, walFileName)
	cleanupPrefetchDirectory(prefetchLocation, timelineId, logSegNo, cleaner)
	cleanupPrefetchDirectory(runningLocation, timelineId, logSegNo, cleaner)
}

func cleanupPrefetchDirectory(directory string, timelineId uint32, logSegNo uint64, cleaner Cleaner) {
	files, err := cleaner.GetFiles(directory)
	if err != nil {
		log.Println("WAL-prefetch cleanup failed, : ", err, " cannot enumerate files in dir: ", directory)
	}

	for _, f := range files {
		fileTimelineId, fileLogSegNo, err := ParseWALFileName(f)
		if err != nil {
			continue
		}
		if fileTimelineId < timelineId || (fileTimelineId == timelineId && fileLogSegNo < logSegNo) {
			cleaner.Remove(path.Join(directory, f))
		}
	}
}
