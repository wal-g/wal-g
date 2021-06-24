package postgres

import (
	"archive/tar"
	"context"
	"os"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type copyStatus int

const (
	possibleCopy copyStatus = iota
	doNotCopy
	fromNew
	processed
)

type fileInfo struct {
	status copyStatus
	info   *ComposeFileInfo
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

	prevBackup             Backup
	newBackupName          string
	tarFileSets            TarFileSets
	copiedTarFileSets      TarFileSets
	tarUnchangedFilesCount map[string]int
	prevFileTar            map[string]string
	prevTarFileSets        TarFileSets
	fileInfo               map[string]*fileInfo
	headerInfos            map[string]*headerInfo
	copiedTars             map[string]bool
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
	prevBackup Backup,
	newBackupName string,
	tarUnchangedFilesCount map[string]int,
	prevFileTar map[string]string,
	prevTarFileSets TarFileSets,
) *CopyTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	prevBackup.GetSentinel()
	return &CopyTarBallComposer{
		tarBallQueue:           tarBallQueue,
		tarFilePacker:          tarBallFilePacker,
		crypter:                crypter,
		files:                  files,
		ctx:                    ctx,
		errorGroup:             errorGroup,
		prevBackup:             prevBackup,
		newBackupName:          newBackupName,
		tarFileSets:            make(TarFileSets),
		copiedTarFileSets:      make(TarFileSets),
		tarUnchangedFilesCount: tarUnchangedFilesCount,
		prevFileTar:            prevFileTar,
		prevTarFileSets:        prevTarFileSets,
		fileInfo:               make(map[string]*fileInfo),
		headerInfos:            make(map[string]*headerInfo),
		copiedTars:             make(map[string]bool),
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
		bundle.Crypter, maker.previousBackup, maker.newBackupName, tarUnchangedFilesCount,
		prevFileTar, prevTarFileSets), nil
}

func (c *CopyTarBallComposer) AddFile(info *ComposeFileInfo) {
	var fileName = info.header.Name
	var currFile = fileInfo{}
	if !c.prevBackup.SentinelDto.Files[fileName].MTime.Equal(info.header.ModTime) {
		c.tarUnchangedFilesCount[c.prevFileTar[fileName]] = -1
		currFile.status = doNotCopy
	} else if _, exists := c.prevFileTar[fileName]; exists {
		c.tarUnchangedFilesCount[c.prevFileTar[fileName]] -= 1
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
	if !c.prevBackup.SentinelDto.Files[fileName].MTime.Equal(info.ModTime()) {
		c.tarUnchangedFilesCount[c.prevFileTar[fileName]] = -1
		currHeader.status = doNotCopy
	} else if _, exists := c.prevFileTar[fileName]; exists {
		c.tarUnchangedFilesCount[c.prevFileTar[fileName]] -= 1
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
	c.prevBackup.Folder.CopyObject(
		utility.BaseBackupPath,
		c.prevBackup.Name+internal.TarPartitionFolderName+tarName,
		c.newBackupName+internal.TarPartitionFolderName+tarName)
	c.copiedTars[tarName] = true
	for _, fileName := range c.prevTarFileSets[tarName] {
		if _, exists := c.fileInfo[fileName]; exists {
			c.fileInfo[fileName].status = processed
			c.copiedTarFileSets[tarName] = append(c.copiedTarFileSets[tarName], fileName)
		} else if _, exists := c.headerInfos[fileName]; exists {
			c.headerInfos[fileName].status = processed
			c.copiedTarFileSets[tarName] = append(c.copiedTarFileSets[tarName], fileName)
		}
	}
}

func (c *CopyTarBallComposer) getTarBall() internal.TarBall {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	_, exists := c.copiedTars[tarBall.Name()]
	for exists {
		c.tarBallQueue.SkipTarBall(tarBall)
		tarBall = c.tarBallQueue.Deque()
		tarBall.SetUp(c.crypter)
		_, exists = c.copiedTars[tarBall.Name()]
	}
	return tarBall
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

	for fileName := range c.fileInfo {
		file := c.fileInfo[fileName]
		if file.status == possibleCopy {
			c.copyTar(c.prevFileTar[fileName])
			file.status = processed
		}
	}
	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		if header.status == possibleCopy {
			c.copyTar(c.prevFileTar[headerName])
			header.status = processed
		}
	}

	tarExpectedSize := int64(0)
	var tarBall internal.TarBall = nil
	for fileName := range c.fileInfo {
		file := c.fileInfo[fileName]
		fileSize := file.info.fileInfo.Size()
		if file.status == doNotCopy || file.status == fromNew {
			if tarExpectedSize+fileSize > c.tarBallQueue.TarSizeThreshold || tarBall == nil {
				tarBall = c.getTarBall()
			}
			c.errorGroup.Go(func() error {
				err := c.tarFilePacker.PackFileIntoTar(file.info, tarBall)
				if err != nil {
					return err
				}
				return nil
			})
			tarExpectedSize += fileSize
			c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], fileName)
			file.status = processed
		}
	}

	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		headerSize := header.info.Size()
		if header.status == doNotCopy || header.status == fromNew {
			if tarExpectedSize+headerSize > c.tarBallQueue.TarSizeThreshold {
				tarBall = c.tarBallQueue.Deque()
				tarBall.SetUp(c.crypter)
			}
			tarBall.TarWriter().WriteHeader(header.fileInfoHeader)
			tarExpectedSize += headerSize
			c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], headerName)
		}
		header.status = processed
	}
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *CopyTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
