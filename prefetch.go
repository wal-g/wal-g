package walg

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

func HandleWALPrefetch(pre *Prefix, walFileName string, location string) {
	var fileName = walFileName
	var err error
	location = path.Dir(location)
	errors := make(chan (interface{}))
	awaited := 0
	for i := 0; i < getMaxConcurrency(8); i++ {
		fileName, err = NextWALFileName(fileName)
		if err != nil {
			log.Println("WAL-prefetch failed: ", err, " file: ", fileName)
		}
		awaited++
		go prefetchFile(location, pre, fileName, errors)
		time.Sleep(time.Millisecond) // ramp up in order
	}

	go cleanupPrefetchDirectories(walFileName, location, FileSystemCleaner{})

	for i := 0; i < awaited; i++ {
		<-errors // Wait until everyone is done. Errors are reported in recovery
	}
}

func prefetchFile(location string, pre *Prefix, walFileName string, error_queue chan (interface{})) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Prefetch unsucessfull ", walFileName, r)
			error_queue <- r
		} else {
			error_queue <- nil
		}
	}()

	_, runningLocation, oldPath, newPath := getPrefetchLocations(location, walFileName)
	_, err_o := os.Stat(oldPath)
	_, err_n := os.Stat(newPath)

	if (err_o == nil || !os.IsNotExist(err_o)) || (err_n == nil || !os.IsNotExist(err_n)) {
		// Seems someone is doing something about this file
		return
	}

	log.Println("WAL-prefetch file: ", walFileName)
	os.MkdirAll(runningLocation, 0755)

	DownloadFile(pre, walFileName, oldPath)

	_, err_o = os.Stat(oldPath)
	_, err_n = os.Stat(newPath)
	if err_o == nil && os.IsNotExist(err_n) {
		os.Link(oldPath, newPath)
	} else {
		os.Remove(oldPath) // error is ignored
	}
}

func getPrefetchLocations(location string, walFileName string) (prefetchLocation string, runningLocation string, runningFile string, fetchedFile string) {
	prefetchLocation = path.Join(location, ".wal-g", "prefetch")
	runningLocation = path.Join(prefetchLocation, "running")
	oldPath := path.Join(runningLocation, walFileName)
	newPath := path.Join(prefetchLocation, walFileName)
	return prefetchLocation, runningLocation, oldPath, newPath
}

func forkPrefetch(walFileName string, location string) {
	if strings.Contains(walFileName, "history") ||
		strings.Contains(walFileName, "partial") ||
		getMaxConcurrency(16) == 1 {
		return // There will be nothing ot prefetch anyway
	}
	cmd := exec.Command(os.Args[0], "wal-prefetch", walFileName, location)
	cmd.Env = os.Environ()
	err := cmd.Start()

	if err != nil {
		log.Println("WAL-prefetch failed: ", err)
	}
}

type Cleaner interface {
	GetFiles(directory string) ([]string, error)
	Remove(file string)
}

type FileSystemCleaner struct{}

func (this FileSystemCleaner) GetFiles(directory string) (files []string, err error) {
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

func (this FileSystemCleaner) Remove(file string) {
	os.Remove(file)
}

func cleanupPrefetchDirectories(walFileName string, location string, cleaner Cleaner) {
	timelineId, logSegNo, err := ParseWALFileName(walFileName)
	if err != nil {
		log.Println("WAL-prefetch cleanup failed: ", err, " file: ", walFileName)
		return
	}
	prefetchLocation, runningLocation, _, _ := getPrefetchLocations(location, walFileName)
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
