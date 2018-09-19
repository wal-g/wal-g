package walg

import (
	"bytes"
	"fmt"
	"github.com/wal-g/wal-g/walparser"
	"sync"
)

type DeltaFileWriterNotFoundError struct {
	filename string
}

func (err DeltaFileWriterNotFoundError) Error() string {
	return fmt.Sprintf("can't file delta file writer for file: '%s'", err.filename)
}

type DeltaFileManager struct {
	dataFolder            DataFolder
	PartFiles             sync.Map
	DeltaFileWriters      sync.Map
	deltaFileWriterWaiter sync.WaitGroup
	canceledWalRecordings chan string
	CanceledDeltaFiles    map[string]bool
	canceledWaiter        sync.WaitGroup
}

func NewDeltaFileManager(dataFolder DataFolder) *DeltaFileManager {
	manager := &DeltaFileManager{
		dataFolder,
		sync.Map{},
		sync.Map{},
		sync.WaitGroup{},
		make(chan string),
		make(map[string]bool),
		sync.WaitGroup{},
	}
	manager.canceledWaiter.Add(1)
	go manager.GetCanceledDeltaFiles()
	return manager
}

func (manager *DeltaFileManager) GetBlockLocationConsumer(deltaFilename string) (chan walparser.BlockLocation, error) {
	if deltaFileWriter, ok := manager.DeltaFileWriters.Load(deltaFilename); ok {
		return deltaFileWriter.(*DeltaFileChanWriter).BlockLocationConsumer, nil
	}
	physicalDeltaFile, err := manager.dataFolder.OpenReadonlyFile(deltaFilename)
	var deltaFile *DeltaFile
	if err != nil {
		if _, ok := err.(*NoSuchFileError); !ok {
			return nil, err
		}
		deltaFile, err = NewDeltaFile(walparser.NewWalParser())
		if err != nil {
			return nil, err
		}
	} else {
		defer physicalDeltaFile.Close()
		deltaFile, err = LoadDeltaFile(physicalDeltaFile)
		if err != nil {
			return nil, err
		}
	}
	deltaFileWriter := NewDeltaFileChanWriter(deltaFile)
	manager.deltaFileWriterWaiter.Add(1)
	go deltaFileWriter.Consume(&manager.deltaFileWriterWaiter)
	manager.DeltaFileWriters.Store(deltaFilename, deltaFileWriter)
	return deltaFileWriter.BlockLocationConsumer, nil
}

func (manager *DeltaFileManager) GetPartFile(deltaFilename string) (*WalPartFile, error) {
	partFilename := ToPartFilename(deltaFilename)
	if partFile, ok := manager.PartFiles.Load(partFilename); ok {
		return partFile.(*WalPartFile), nil
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
		partFile, err = LoadPartFile(physicalPartFile)
		if err != nil {
			return nil, err
		}
	}
	manager.PartFiles.Store(partFilename, partFile)
	return partFile, nil
}

func (manager *DeltaFileManager) FlushPartFiles() (completedPartFiles map[string]bool) {
	close(manager.canceledWalRecordings)
	manager.canceledWaiter.Wait()
	completedPartFiles = make(map[string]bool)
	manager.PartFiles.Range(func(key, value interface{}) bool {
		partFilename := key.(string)
		partFile := value.(*WalPartFile)
		deltaFilename := partFilenameToDelta(partFilename)
		if _, ok := manager.CanceledDeltaFiles[deltaFilename]; ok {
			return true
		}
		if partFile.IsComplete() {
			completedPartFiles[partFilename] = true
			err := manager.CombinePartFile(deltaFilename, partFile)
			if err != nil {
				manager.CanceledDeltaFiles[deltaFilename] = true
				fmt.Printf("canceled delta file writing because of error: %v", err)
			}
		} else {
			err := saveToDataFolder(partFile, partFilename, manager.dataFolder)
			if err != nil {
				manager.CanceledDeltaFiles[deltaFilename] = true
				fmt.Printf("failed to save part file: '%s' because of error: '%v'", partFilename, err)
			}
		}
		return true
	})
	return
}

func (manager *DeltaFileManager) FlushDeltaFiles(uploader *Uploader, completedPartFiles map[string]bool) {
	manager.DeltaFileWriters.Range(func(key, value interface{}) bool {
		deltaFileWriter := value.(*DeltaFileChanWriter)
		deltaFileWriter.close()
		return true
	})
	manager.deltaFileWriterWaiter.Wait()
	manager.DeltaFileWriters.Range(func(key, value interface{}) bool {
		deltaFilename := key.(string)
		deltaFileWriter := value.(*DeltaFileChanWriter)
		if _, ok := manager.CanceledDeltaFiles[deltaFilename]; ok {
			return true
		}
		partFilename := ToPartFilename(deltaFilename)
		if _, ok := completedPartFiles[partFilename]; ok {
			var deltaFileData bytes.Buffer
			err := deltaFileWriter.DeltaFile.Save(&deltaFileData)
			if err != nil {
				fmt.Printf("failed to upload delta file: '%s' because of saving error: '%v'", deltaFilename, err)
			} else {
				err = uploader.UploadFile(&NamedReaderImpl{&deltaFileData, deltaFilename})
				if err != nil {
					fmt.Printf("failed to upload delta file: '%s' because of uploading error: '%v'", deltaFilename, err)
				}
			}
		} else {
			err := saveToDataFolder(deltaFileWriter.DeltaFile, deltaFilename, manager.dataFolder)
			if err != nil {
				fmt.Printf("failed to save delta file: '%s' because of error: '%v'", deltaFilename, err)
			}
		}
		return true
	})
}

func (manager *DeltaFileManager) FlushFiles(uploader *Uploader) {
	manager.dataFolder.CleanFolder()
	completedPartFiles := manager.FlushPartFiles()
	manager.FlushDeltaFiles(uploader, completedPartFiles)
}

func (manager *DeltaFileManager) CancelRecording(walFilename string) {
	manager.canceledWalRecordings <- walFilename
}

func (manager *DeltaFileManager) GetCanceledDeltaFiles() {
	for walFilename := range manager.canceledWalRecordings {
		deltaFilename, err := GetDeltaFilenameFor(walFilename)
		if err != nil {
			continue
		}
		manager.CanceledDeltaFiles[deltaFilename] = true
		nextWalFilename, _ := GetNextWalFilename(walFilename)
		deltaFilename, _ = GetDeltaFilenameFor(nextWalFilename)
		manager.CanceledDeltaFiles[deltaFilename] = true
	}
	manager.canceledWaiter.Done()
}

func (manager *DeltaFileManager) CombinePartFile(deltaFilename string, partFile *WalPartFile) error {
	deltaFileWriterInterface, ok := manager.DeltaFileWriters.Load(deltaFilename)
	if !ok {
		return DeltaFileWriterNotFoundError{deltaFilename}
	}
	deltaFileWriter := deltaFileWriterInterface.(*DeltaFileChanWriter)
	deltaFileWriter.DeltaFile.WalParser = walparser.LoadWalParserFromCurrentRecordData(partFile.WalHeads[WalFileInDelta-1])
	var err error
	records, err := partFile.CombineRecords()
	if err != nil {
		return err
	}
	locations := ExtractBlockLocations(records)
	for _, location := range locations {
		deltaFileWriter.BlockLocationConsumer <- location
	}
	return nil
}
