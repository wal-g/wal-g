package postgres

import (
	"archive/tar"
	"context"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/parallel"
	"golang.org/x/sync/errgroup"
)

type copyStatus int

const (
	possibleCopy copyStatus = iota // Mark files in previous tarball when it unclear whether tarball could be copied
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
	files         *parallel.RegularBundleFiles
	ctx           context.Context
	errorGroup    *errgroup.Group

	copyCount              int
	prevBackup             Backup
	newBackupName          string
	tarFileSets            parallel.TarFileSets
	tarUnchangedFilesCount map[string]int
	prevFileTar            map[string]string
	prevTarFileSets        parallel.TarFileSets
	fileInfo               map[string]*fileInfo
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
	files *parallel.RegularBundleFiles,
	crypter crypto.Crypter,
	prevBackup Backup,
	newBackupName string,
	tarUnchangedFilesCount map[string]int,
	prevFileTar map[string]string,
	prevTarFileSets parallel.TarFileSets,
) (*CopyTarBallComposer, error) {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	_, _, err := prevBackup.GetSentinelAndFilesMetadata()
	if err != nil {
		return nil, err
	}
	return &CopyTarBallComposer{
		tarBallQueue:           tarBallQueue,
		tarFilePacker:          tarBallFilePacker,
		crypter:                crypter,
		files:                  files,
		ctx:                    ctx,
		errorGroup:             errorGroup,
		copyCount:              0,
		prevBackup:             prevBackup,
		newBackupName:          newBackupName,
		tarFileSets:            parallel.NewRegularTarFileSets(),
		tarUnchangedFilesCount: tarUnchangedFilesCount,
		prevFileTar:            prevFileTar,
		prevTarFileSets:        prevTarFileSets,
		fileInfo:               make(map[string]*fileInfo),
		headerInfos:            make(map[string]*headerInfo),
	}, nil
}

func (maker *CopyTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	prevFileTar := make(map[string]string)
	prevTarFileSets := parallel.NewRegularTarFileSets()
	tarUnchangedFilesCount := make(map[string]int)
	if maker.previousBackup.SentinelDto != nil {
		for tarName, fileSet := range maker.previousBackup.FilesMetadataDto.TarFileSets {
			for _, fileName := range fileSet {
				prevFileTar[fileName] = tarName
				prevTarFileSets.AddFile(tarName, fileName)
			}
			tarUnchangedFilesCount[tarName] = len(fileSet)
		}
	} else {
		return nil, errors.New("No SentinelDto in previous backup")
	}
	files := &parallel.RegularBundleFiles{}
	tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap,
		bundle.IncrementFromLsn, files, maker.filePackerOptions)
	return NewCopyTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, files,
		bundle.Crypter, maker.previousBackup, maker.newBackupName, tarUnchangedFilesCount,
		prevFileTar, prevTarFileSets)
}

func (c *CopyTarBallComposer) AddFile(info *ComposeFileInfo) {
	var fileName = info.header.Name
	var currFile = fileInfo{}
	if _, exists := c.prevFileTar[fileName]; exists {
		if !c.prevBackup.FilesMetadataDto.Files[fileName].MTime.Equal(info.header.ModTime) {
			currFile.status = doNotCopy
		} else {
			c.tarUnchangedFilesCount[c.prevFileTar[fileName]]--
			currFile.status = possibleCopy
		}
	} else {
		currFile.status = fromNew
	}
	currFile.info = info
	c.fileInfo[fileName] = &currFile
}

func (c *CopyTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	var fileName = fileInfoHeader.Name
	var currHeader = headerInfo{}
	if _, exists := c.prevFileTar[fileName]; exists {
		if !c.prevBackup.FilesMetadataDto.Files[fileName].MTime.Equal(info.ModTime()) {
			currHeader.status = doNotCopy
		} else {
			c.tarUnchangedFilesCount[c.prevFileTar[fileName]]--
			currHeader.status = possibleCopy
		}
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

func (c *CopyTarBallComposer) copyTar(tarName string) error {
	tracelog.InfoLogger.Printf("Copying %s ...\n", tarName)
	splitTarName := strings.Split(tarName, ".")
	fileExtension := splitTarName[len(splitTarName)-1]
	newTarName := "copy_" + strconv.Itoa(c.copyCount) + ".tar." + fileExtension
	c.copyCount++
	srcPath := path.Join(c.prevBackup.Name, internal.TarPartitionFolderName, tarName)
	dstPath := path.Join(c.newBackupName, internal.TarPartitionFolderName, newTarName)
	err := c.prevBackup.Folder.CopyObject(srcPath, dstPath)
	if err != nil {
		return err
	}
	for _, fileName := range c.prevTarFileSets.Get()[tarName] {
		if file, exists := c.fileInfo[fileName]; exists {
			file.status = processed
			c.tarFileSets.AddFile(newTarName, fileName)
			c.files.AddFile(file.info.header, file.info.fileInfo, file.info.isIncremented)
		} else if header, exists := c.headerInfos[fileName]; exists {
			header.status = processed
			c.tarFileSets.AddFile(newTarName, fileName)
			c.files.AddFile(header.fileInfoHeader, header.info, false)
		}
	}
	return nil
}

func (c *CopyTarBallComposer) getTarBall() internal.TarBall {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	return tarBall
}

func (c *CopyTarBallComposer) copyUnchangedTars() error {
	for tarName, cnt := range c.tarUnchangedFilesCount {
		if cnt != 0 {
			for _, fileName := range c.prevTarFileSets.Get()[tarName] {
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
			err := c.copyTar(c.prevFileTar[fileName])
			if err != nil {
				return err
			}
		}
	}
	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		if header.status == possibleCopy {
			err := c.copyTar(c.prevFileTar[headerName])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CopyTarBallComposer) PackTarballs() (parallel.TarFileSets, error) {
	err := c.copyUnchangedTars()
	if err != nil {
		return nil, err
	}
	var tarBall internal.TarBall = nil
	for fileName := range c.fileInfo {
		file := c.fileInfo[fileName]
		if file.status == doNotCopy || file.status == fromNew {
			tarBall = c.getTarBall()
			c.errorGroup.Go(func() error {
				err := c.tarFilePacker.PackFileIntoTar(file.info, tarBall)
				if err != nil {
					return err
				}
				return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
			})
			c.tarFileSets.AddFile(tarBall.Name(), fileName)
			file.status = processed
		}
	}

	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		if header.status == doNotCopy || header.status == fromNew {
			tarBall = c.getTarBall()
			err := tarBall.TarWriter().WriteHeader(header.fileInfoHeader)
			if err != nil {
				return nil, err
			}
			c.tarFileSets.AddFile(tarBall.Name(), headerName)
			c.files.AddFile(header.fileInfoHeader, header.info, false)
			c.tarBallQueue.EnqueueBack(tarBall)
		}
		header.status = processed
	}
	err = c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *CopyTarBallComposer) GetFiles() parallel.BundleFiles {
	return c.files
}
