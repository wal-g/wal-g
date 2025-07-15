package postgres

import (
	"archive/tar"
	"io"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFileNew(fileReader io.Reader,
	header *tar.Header,
	targetPath string,
	fsync bool) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[header.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", header.Name)
			return nil
		}
	}
	fileUnwrapper := getFileUnwrapper(tarInterpreter, header, targetPath)
	localFile, isNewFile, err := utility.GetLocalFile(targetPath, header)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	defer utility.LoggedSync(localFile, "", fsync)
	var unwrapResult *FileUnwrapResult
	var unwrapError error
	if isNewFile {
		unwrapResult, unwrapError = fileUnwrapper.UnwrapNewFile(fileReader, header, localFile, fsync)
	} else {
		unwrapResult, unwrapError = fileUnwrapper.UnwrapExistingFile(fileReader, header, localFile, fsync)
	}
	if unwrapError != nil {
		return unwrapError
	}
	tarInterpreter.AddFileUnwrapResult(unwrapResult, header.Name)
	return nil
}

// get file unwrapper for file depending on backup type
func getFileUnwrapper(tarInterpreter *FileTarInterpreter, header *tar.Header, targetPath string) IBackupFileUnwrapper {
	fileDescription, haveFileDescription := tarInterpreter.FilesMetadata.Files[header.Name]
	isIncremented := haveFileDescription && fileDescription.IsIncremented
	var isPageFile bool
	if localFileInfo, _ := utility.GetLocalFileInfo(targetPath); localFileInfo != nil {
		isPageFile = isPagedFile(localFileInfo, targetPath)
	}
	options := &BackupFileOptions{isIncremented: isIncremented, isPageFile: isPageFile}

	// todo: clearer catchup backup detection logic
	isCatchup := tarInterpreter.createNewIncrementalFiles
	if isCatchup {
		return NewFileUnwrapper(CatchupBackupFileUnwrapper, options)
	}
	return NewFileUnwrapper(DefaultBackupFileUnwrapper, options)
}

func (tarInterpreter *FileTarInterpreter) AddFileUnwrapResult(result *FileUnwrapResult, fileName string) {
	switch result.FileUnwrapResultType {
	case Skipped:
		return
	case Completed:
		tarInterpreter.addToCompletedFiles(fileName)
	case CreatedFromIncrement:
		tarInterpreter.addToCreatedPageFiles(fileName, result.blockCount)
	case WroteIncrementBlocks:
		tarInterpreter.addToWrittenIncrementFiles(fileName, result.blockCount)
	}
}

func (tarInterpreter *FileTarInterpreter) addToCompletedFiles(fileName string) {
	tarInterpreter.UnwrapResult.completedFilesMutex.Lock()
	tarInterpreter.UnwrapResult.completedFiles = append(tarInterpreter.UnwrapResult.completedFiles, fileName)
	tarInterpreter.UnwrapResult.completedFilesMutex.Unlock()
}

func (tarInterpreter *FileTarInterpreter) addToCreatedPageFiles(fileName string, blocksToRestoreCount int64) {
	tarInterpreter.UnwrapResult.createdPageFilesMutex.Lock()
	tarInterpreter.UnwrapResult.createdPageFiles[fileName] = blocksToRestoreCount
	tarInterpreter.UnwrapResult.createdPageFilesMutex.Unlock()
}

func (tarInterpreter *FileTarInterpreter) addToWrittenIncrementFiles(fileName string, writtenBlocksCount int64) {
	tarInterpreter.UnwrapResult.writtenIncrementFilesMutex.Lock()
	tarInterpreter.UnwrapResult.writtenIncrementFiles[fileName] = writtenBlocksCount
	tarInterpreter.UnwrapResult.writtenIncrementFilesMutex.Unlock()
}
