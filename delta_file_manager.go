package walg

import (
	"os"
	"github.com/wal-g/wal-g/walparser"
	"log"
	"bytes"
)

// TODO : clean up directory from outdated delta files
type DeltaFileManager struct {
	dataFolder *TemporaryDataFolder
	partFiles map[string] *WalPartFile
	deltaFileWriters map[string] *DeltaFileChanWriter
	canceledWalRecordings chan string
}

func NewDeltaFileManager(dataFolder *TemporaryDataFolder) *DeltaFileManager {
	return &DeltaFileManager{
		dataFolder,
		make(map[string] *WalPartFile),
		make(map[string] *DeltaFileChanWriter),
		make(chan string),
	}
}

// TODO : unit tests
func (manager *DeltaFileManager) getBlockLocationConsumer(deltaFilename string) (chan walparser.BlockLocation, error) {
	if deltaFileWriter, ok := manager.deltaFileWriters[deltaFilename]; ok {
		return deltaFileWriter.blockLocationConsumer, nil
	}
	physicalDeltaFile, err := manager.dataFolder.openReadonlyFile(deltaFilename)
	var deltaFile *DeltaFile
	if err != nil {
		if !os.IsNotExist(err) {
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
	go deltaFileWriter.consume()
	manager.deltaFileWriters[deltaFilename] = deltaFileWriter
	return deltaFileWriter.blockLocationConsumer, nil
}

// TODO : unit tests
func (manager *DeltaFileManager) getPartFile(deltaFilename string) (*WalPartFile, error) {
	partFilename := toPartFilename(deltaFilename)
	if partFile, ok := manager.partFiles[partFilename]; ok {
		return partFile, nil
	}
	physicalPartFile, err := manager.dataFolder.openReadonlyFile(partFilename)
	var partFile *WalPartFile
	if err != nil {
		if !os.IsNotExist(err) {
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
func (manager *DeltaFileManager) FlushFiles() {
	canceledDeltaFiles := manager.getCanceledDeltaFiles()
	completedPartFiles := make(map[string]bool)
	for partFilename, partFile := range manager.partFiles {
		deltaFilename := partFilenameToDelta(partFilename)
		if _, ok := canceledDeltaFiles[deltaFilename]; ok {
			continue
		}
		if partFile.isComplete() {
			completedPartFiles[partFilename] = true
			err := manager.combinePartFile(deltaFilename, partFile)
			if err != nil {
				canceledDeltaFiles[deltaFilename] = true
				log.Printf("canceled delta file writing because of error: %v", err)
			}
		} else {
			err := saveToDataFolder(partFile, partFilename, manager.dataFolder)
			if err != nil {
				canceledDeltaFiles[deltaFilename] = true
				log.Printf("failed to save part file: '%s' because of error: '%v'", partFilename, err)
			}
		}
	}
	for deltaFilename, deltaFileWriter := range manager.deltaFileWriters {
		deltaFileWriter.close()
		if _, ok := canceledDeltaFiles[deltaFilename]; ok {
			continue
		}
		partFilename := toPartFilename(deltaFilename)
		if _, ok := completedPartFiles[partFilename]; ok {
			// TODO : upload file to S3
		} else {
			err := saveToDataFolder(deltaFileWriter.deltaFile, deltaFilename, manager.dataFolder)
			if err != nil {
				log.Printf("failed to save delta file: '%s' because of error: '%v'", deltaFilename, err)
			}
		}
	}
}

func (manager *DeltaFileManager) cancelRecording(walFilename string) {
	manager.canceledWalRecordings <- walFilename
}

// TODO : unit tests
func (manager *DeltaFileManager) getCanceledDeltaFiles() map[string]bool {
	canceledDeltaFiles := make(map[string]bool)
	close(manager.canceledWalRecordings)
	for walFilename := range manager.canceledWalRecordings {
		canceledDeltaFiles[toDeltaFilename(walFilename)] = true
		nextWalFilename, err := GetNextWalFilename(walFilename)
		if err != nil {
			canceledDeltaFiles[toDeltaFilename(nextWalFilename)] = true
		} else {
			log.Printf("error: %v", err)
		}
	}
	return canceledDeltaFiles
}

// TODO : unit tests
func (manager *DeltaFileManager) combinePartFile(deltaFilename string, partFile *WalPartFile) error {
	deltaFileWriter := manager.deltaFileWriters[deltaFilename]
	var err error
	deltaFileWriter.deltaFile.walParser, err = walparser.LoadParser(bytes.NewReader(partFile.walHeads[WalFileInDelta - 1]))
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
