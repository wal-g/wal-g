package postgres

import (
	"archive/tar"
	"context"
	"os"

	"github.com/wal-g/storages/storage"	
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal"
	"golang.org/x/sync/errgroup"
)

type copyStatus int

const (
	possibleCopy copyStatus = iota
	doNotCopy
	fromNew
)

type fileInfo struct {
	status copyStatus
	info *ComposeFileInfo
}

type headerInfo struct {
	status         copyStatus
	fileInfoHeader *tar.Header
	info           os.FileInfo
}

type CopyTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *RegularBundleFiles
	ctx           context.Context
	errorGroup    *errgroup.Group

	prevFolder             storage.Folder
	newBackupName          string
	tarFileSets            TarFileSets
	tarUnchangedFilesCount map[string]int
	prevFileTar            map[string]string
	prevTarFileSets        TarFileSets
	fileInfo              map[string]*fileInfo
	headerInfos            map[string]*headerInfo
}

type CopyTarBallComposerMaker struct {
	previousBackup    Backup
	newBackupName     string
	filePackerOptions TarBallFilePackerOptions
}

func NewCopyTarBallComposerMaker(previousBackup Backup, newBackupName string,
	                             filePackerOptions TarBallFilePackerOptions) *CopyTarBallComposerMaker {
	return &CopyTarBallComposerMaker{previousBackup, newBackupName, filePackerOptions}
}

func NewCopyTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *RegularBundleFiles,
	crypter crypto.Crypter,
	prevFolder storage.Folder,
	newBackupName string,
	tarUnchangedFilesCount map[string]int,
	prevFileTar map[string]string,
	prevTarFileSets TarFileSets,
) *CopyTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	return &CopyTarBallComposer{
		tarBallQueue:           tarBallQueue,
		tarFilePacker:          tarBallFilePacker,
		crypter:                crypter,
		files:                  files,
		ctx:                    ctx,
		errorGroup:             errorGroup,
		prevFolder:             prevFolder,
		newBackupName:          newBackupName,
		tarFileSets:            make(TarFileSets),
		tarUnchangedFilesCount: tarUnchangedFilesCount,
		prevFileTar:            prevFileTar,
		prevTarFileSets:        prevTarFileSets,
		fileInfo:              make(map[string]*fileInfo),
		headerInfos:            make(map[string]*headerInfo),
	}
}

func (maker *CopyTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	prevFileTar := make(map[string]string)
	prevTarFileSets := make(map[string][]string)
	tarUnchangedFilesCount := make(map[string]int)
	for tarName, fileSet := range maker.previousBackup.SentinelDto.TarFileSets {
		for _, fileName := range fileSet {
			prevFileTar[fileName] = tarName
			prevTarFileSets[tarName] = append(prevTarFileSets[tarName], fileName)
		}
		tarUnchangedFilesCount[tarName] = len(fileSet)
	}
	files := &RegularBundleFiles{}
	tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap,
		bundle.IncrementFromLsn, files, maker.filePackerOptions)
	return NewCopyTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, files,
		bundle.Crypter, maker.previousBackup.Folder, maker.newBackupName, tarUnchangedFilesCount,
		prevFileTar, prevTarFileSets), nil
}

func (c *CopyTarBallComposer) AddFile(info *ComposeFileInfo) {
	var fileName = info.header.Name
	var currFile = fileInfo{}
	// f, _ := os.OpenFile("./log_file.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	// defer f.Close()
	// f.WriteString(fileName+"\n")
	if info.isChanged {
		c.tarUnchangedFilesCount[c.prevFileTar[fileName]] = -1
		currFile.status = doNotCopy
	} else if _, exists := c.prevFileTar[fileName]; exists {
		c.tarUnchangedFilesCount[fileName] -= 1
		currFile.status = possibleCopy
	} else {
		currFile.status = fromNew
	}
	currFile.info = info
	c.fileInfo[fileName] = &currFile
}

func (c *CopyTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	var fileName = fileInfoHeader.Name
	var currHeader = headerInfo{}
	// f, _ := os.OpenFile("./log_header.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	// defer f.Close()
	// f.WriteString(fileName+"\n")
	if _, exists := c.prevFileTar[fileName]; exists {
		c.tarUnchangedFilesCount[fileName] -= 1
		currHeader.status = possibleCopy
	} else {
		currHeader.status = fromNew
	}
	currHeader.info = info
	currHeader.fileInfoHeader = fileInfoHeader
	c.files.AddFile(fileInfoHeader, info, false)
	c.headerInfos[fileName] = &currHeader
	return nil
}

func (c *CopyTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *CopyTarBallComposer) copyTar(tarName string) {
	c.prevFolder.CopyObject(tarName, c.prevFolder.GetPath()+"/"+c.newBackupName)
}

func (c *CopyTarBallComposer) PackTarballs() (TarFileSets, error) {
	for tarName, cnt := range c.tarUnchangedFilesCount {
		if cnt != 0 {
			for _, fileName := range c.prevTarFileSets[tarName] {
				if _, exists := c.fileInfo[fileName]; exists {
					c.fileInfo[fileName].status = doNotCopy
				} else if _, exists := c.headerInfos[fileName]; exists {
					c.headerInfos[fileName].status = doNotCopy
				}
			}
		}
	}
	tarExpectedSize := int64(0)
	tarBall := c.tarBallQueue.Deque()
	for fileName := range c.fileInfo {
		file := c.fileInfo[fileName]
		fileSize := file.info.fileInfo.Size()
		if file.status == possibleCopy {
			c.copyTar(c.prevFileTar[fileName])
		} else if tarExpectedSize+fileSize > c.tarBallQueue.TarSizeThreshold {
			tarBall = c.tarBallQueue.Deque()
		} else {
			c.errorGroup.Go(func() error {
				err := c.tarFilePacker.PackFileIntoTar(file.info, tarBall)
				if err != nil {
					return err
				}
				return nil
			})
			tarExpectedSize += fileSize
			c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], fileName)
		}
	}
	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		headerSize := header.info.Size()
		if header.status == possibleCopy {
			c.copyTar(c.prevFileTar[headerName])
		} else if tarExpectedSize+headerSize > c.tarBallQueue.TarSizeThreshold {
			tarBall = c.tarBallQueue.Deque()
		} else {
			tarBall.TarWriter().WriteHeader(header.fileInfoHeader)
			tarExpectedSize += headerSize
			c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], headerName)
		}
	}
	return c.tarFileSets, nil
}

func (c *CopyTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
