package internal

import (
	"github.com/tinsane/tracelog"
	"path"
)

// Cleaner interface serves to separate file system logic from prefetch clean logic to make it testable
type Cleaner interface {
	getFiles(directory string) ([]string, error)
	remove(file string)
}

func CleanupPrefetchDirectories(walFileName string, location string, cleaner Cleaner) {
	timelineId, logSegNo, err := ParseWALFilename(walFileName)
	if err != nil {
		tracelog.WarningLogger.Println("WAL-prefetch cleanup failed: ", err, " file: ", walFileName)
		return
	}
	prefetchLocation, runningLocation, _, _ := GetPrefetchLocations(location, walFileName)
	for _, cleaningLocation := range []string{prefetchLocation, runningLocation} {
		cleanupPrefetchDirectory(cleaningLocation, timelineId, logSegNo, cleaner)
	}
}

// TODO : unit tests
func cleanupPrefetchDirectory(directory string, timelineId uint32, logSegNo uint64, cleaner Cleaner) {
	files, err := cleaner.getFiles(directory)
	if err != nil {
		tracelog.WarningLogger.Println("WAL-prefetch cleanup failed, : ", err, " cannot enumerate files in dir: ", directory)
	}

	for _, f := range files {
		fileTimelineId, fileLogSegNo, err := ParseWALFilename(f)
		if err != nil {
			continue
		}
		if fileTimelineId < timelineId || (fileTimelineId == timelineId && fileLogSegNo < logSegNo) {
			cleaner.remove(path.Join(directory, f))
		}
	}
}
