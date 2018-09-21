package walg

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"
)

// TODO : unit tests
// HandleWALPrefetch is invoked by wal-fetch command to speed up database restoration
func HandleWALPrefetch(folder *S3Folder, walFileName string, location string) {
	var fileName = walFileName
	var err error
	location = path.Dir(location)
	waitGroup := &sync.WaitGroup{}
	for i := 0; i < getMaxDownloadConcurrency(8); i++ {
		fileName, err = GetNextWalFilename(fileName)
		if err != nil {
			log.Println("WAL-prefetch failed: ", err, " file: ", fileName)
		}
		waitGroup.Add(1)
		go prefetchFile(location, folder, fileName, waitGroup)
		time.Sleep(10 * time.Millisecond) // ramp up in order
	}

	go CleanupPrefetchDirectories(walFileName, location, FileSystemCleaner{})

	waitGroup.Wait()
}

// TODO : unit tests
func prefetchFile(location string, folder *S3Folder, walFileName string, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Prefetch unsuccessful ", walFileName, r)
		}
		waitGroup.Done()
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

	err := downloadWALFileTo(folder, walFileName, oldPath)
	if err != nil {
		log.Fatalf("%v+\n", err)
	}

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

// TODO : unit tests
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
