package walg

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser"
	"log"
	"sync"
)

// TODO : clean up directory from outdated delta files
type DeltaFileManager struct {
	dataFolder            DataFolder
	partFiles             map[string]*WalPartFile
	deltaFileWriters      map[string]*DeltaFileChanWriter
	deltaFileWriterWaiter sync.WaitGroup
	canceledWalRecordings chan string
	canceledDeltaFiles    map[string]bool
	canceledWaiter        sync.WaitGroup
}

func NewDeltaFileManager(dataFolder DataFolder) *DeltaFileManager {
	manager := &DeltaFileManager{
		dataFolder,
		make(map[string]*WalPartFile),
		make(map[string]*DeltaFileChanWriter),
		sync.WaitGroup{},
		make(chan string),
		make(map[string]bool),
		sync.WaitGroup{},
	}
	manager.canceledWaiter.Add(1)
	go manager.getCanceledDeltaFiles()
	return manager
}

// TODO : unit tests
func (manager *DeltaFileManager) getBlockLocationConsumer(deltaFilename string) (chan walparser.BlockLocation, error) {
	if deltaFileWriter, ok := manager.deltaFileWriters[deltaFilename]; ok {
		return deltaFileWriter.blockLocationConsumer, nil
	}
	physicalDeltaFile, err := manager.dataFolder.OpenReadonlyFile(deltaFilename)
	var deltaFile *DeltaFile
	if err != nil {
		if _, ok := err.(*NoSuchFileError); !ok {
			return nil, err
		}
		deltaFile = NewDeltaFile(walparser.NewWalParser())
	} else {
		defer physicalDeltaFile.Close()
		deltaFile, err = loadDeltaFile(physicalDeltaFile)
		if err != nil {
			return nil, err
		}
	}
	deltaFileWriter := NewDeltaFileChanWriter(deltaFile)
	manager.deltaFileWriterWaiter.Add(1)
	go deltaFileWriter.consume(&manager.deltaFileWriterWaiter)
	manager.deltaFileWriters[deltaFilename] = deltaFileWriter
	return deltaFileWriter.blockLocationConsumer, nil
}

// TODO : unit tests
func (manager *DeltaFileManager) getPartFile(deltaFilename string) (*WalPartFile, error) {
	partFilename := toPartFilename(deltaFilename)
	if partFile, ok := manager.partFiles[partFilename]; ok {
		return partFile, nil
	}
	physicalPartFile, err := manager.dataFolder.OpenReadonlyFile(partFilename)
	var partFile *WalPartFile
	if err != nil {
		if _, ok := err.(*NoSuchFileError); !ok {
			return nil, err
		}
		partFile = NewWalPartFile()
	} else {
		defer physicalPartFile.Close()
		partFile, err = loadPartFile(physicalPartFile)
		if err != nil {
			return nil, err
		}
	}
	manager.partFiles[partFilename] = partFile
	return partFile, nil
}

// TODO : unit tests
func (manager *DeltaFileManager) flushPartFiles() (completedPartFiles map[string]bool) {
	close(manager.canceledWalRecordings)
	manager.canceledWaiter.Wait()
	completedPartFiles = make(map[string]bool)
	for partFilename, partFile := range manager.partFiles {
		deltaFilename := partFilenameToDelta(partFilename)
		if _, ok := manager.canceledDeltaFiles[deltaFilename]; ok {
			continue
		}
		if partFile.isComplete() {
			completedPartFiles[partFilename] = true
			err := manager.combinePartFile(deltaFilename, partFile)
			if err != nil {
				manager.canceledDeltaFiles[deltaFilename] = true
				log.Printf("canceled delta file writing because of error: %v", err)
			}
		} else {
			err := saveToDataFolder(partFile, partFilename, manager.dataFolder)
			if err != nil {
				manager.canceledDeltaFiles[deltaFilename] = true
				log.Printf("failed to save part file: '%s' because of error: '%v'", partFilename, err)
			}
		}
	}
	return
}

// TODO : unit tests
func (manager *DeltaFileManager) flushDeltaFiles(uploader *Uploader, completedPartFiles map[string]bool) {
	for _, deltaFileWriter := range manager.deltaFileWriters {
		deltaFileWriter.close()
	}
	manager.deltaFileWriterWaiter.Wait()
	for deltaFilename, deltaFileWriter := range manager.deltaFileWriters {
		if _, ok := manager.canceledDeltaFiles[deltaFilename]; ok {
			continue
		}
		partFilename := toPartFilename(deltaFilename)
		if _, ok := completedPartFiles[partFilename]; ok {
			var deltaFileData bytes.Buffer
			err := deltaFileWriter.deltaFile.save(&deltaFileData)
			if err != nil {
				log.Printf("failed to upload delta file: '%s' because of saving error: '%v'", deltaFilename, err)
			} else {
				err = uploader.UploadFile(&NamedReaderImpl{&deltaFileData, deltaFilename})
				if err != nil {
					log.Printf("failed to upload delta file: '%s' because of uploading error: '%v'", deltaFilename, err)
				}
			}
		} else {
			err := saveToDataFolder(deltaFileWriter.deltaFile, deltaFilename, manager.dataFolder)
			if err != nil {
				log.Printf("failed to save delta file: '%s' because of error: '%v'", deltaFilename, err)
			}
		}
	}
}

// TODO : unit tests
func (manager *DeltaFileManager) FlushFiles(uploader *Uploader) {
	completedPartFiles := manager.flushPartFiles()
	manager.flushDeltaFiles(uploader, completedPartFiles)
}

func (manager *DeltaFileManager) cancelRecording(walFilename string) {
	manager.canceledWalRecordings <- walFilename
}

// TODO : unit tests
func (manager *DeltaFileManager) getCanceledDeltaFiles() {
	for walFilename := range manager.canceledWalRecordings {
		deltaFilename, err := GetDeltaFilenameFor(walFilename)
		if err != nil {
			continue
		}
		manager.canceledDeltaFiles[deltaFilename] = true
		nextWalFilename, _ := GetNextWalFilename(walFilename)
		deltaFilename, _ = GetDeltaFilenameFor(nextWalFilename)
		manager.canceledDeltaFiles[deltaFilename] = true
	}
	manager.canceledWaiter.Done()
}

// TODO : unit tests
func (manager *DeltaFileManager) combinePartFile(deltaFilename string, partFile *WalPartFile) error {
	deltaFileWriter := manager.deltaFileWriters[deltaFilename]
	var err error
	deltaFileWriter.deltaFile.walParser, err = walparser.LoadParser(bytes.NewReader(partFile.walHeads[WalFileInDelta-1]))
	if err != nil {
		return err
	}
	records, err := partFile.combineRecords()
	locations := extractBlockLocations(records)
	for _, location := range locations {
		deltaFileWriter.blockLocationConsumer <- location
	}
	return nil
}
