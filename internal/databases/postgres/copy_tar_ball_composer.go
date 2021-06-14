package postgres

import (
	"archive/tar"
	"context"
	"os"

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
	status copyStatus
	fileInfoHeader *tar.Header
	info os.FileInfo
}

type CopyTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *RegularBundleFiles
	ctx           context.Context
	errorGroup    *errgroup.Group

	tarFileSets   TarFileSets
	tarUnchangedFilesCount map[string]int
	prevFileTar map[string]string
	prevTarFileSets TarFileSets
	flieInfos map[string]*fileInfo
	headerInfos map[string]*headerInfo
}

type CopyTarBallComposerMaker struct {
	previousBackup Backup
}

func NewCopyTarBallComposerMaker(previousBackup Backup) *CopyTarBallComposerMaker {
	return &CopyTarBallComposerMaker{previousBackup}
}

func NewCopyTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *RegularBundleFiles,
	crypter crypto.Crypter,
) *CopyTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	return &CopyTarBallComposer{
		tarBallQueue:           tarBallQueue,
		tarFilePacker:          tarBallFilePacker,
		crypter:                crypter,
		files:                  files,
		ctx:                    ctx,
		errorGroup:             errorGroup,
		tarFileSets:            make(TarFileSets),
		tarUnchangedFilesCount: make(map[string]int),
		prevFileTar:            make(map[string]string),
	    prevTarFileSets:        make(map[string][]string),
	    flieInfos:              make(map[string]*fileInfo),
	    headerInfos:            make(map[string]*headerInfo),
	}
}

func (maker *CopyTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	var composer CopyTarBallComposer
	for tarName, fileSet := range maker.previousBackup.SentinelDto.TarFileSets {
		for _, fileName := range fileSet {
			composer.prevFileTar[fileName] = tarName
			composer.prevTarFileSets[tarName] = append(composer.prevTarFileSets[tarName], fileName)
		}
		composer.tarUnchangedFilesCount[tarName] = len(fileSet)
	}
	return &composer, nil
}

func (c *CopyTarBallComposer) AddFile(info *ComposeFileInfo) {
	var fileName = info.header.Name
	var currFile = fileInfo{}
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
	c.flieInfos[fileName] = &currFile
}

func (c *CopyTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	var fileName = fileInfoHeader.Name
	var currHeader = headerInfo{}
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

}

func (c *CopyTarBallComposer) PackTarballs() (TarFileSets, error) {
	for tarName, cnt := range(c.tarUnchangedFilesCount) {
		if cnt != 0 {
			for _, fileName := range(c.prevTarFileSets[tarName]) {
				if _, exists := c.flieInfos[fileName]; exists {
					c.flieInfos[fileName].status = doNotCopy
				} else if _, exists := c.headerInfos[fileName]; exists {
					c.headerInfos[fileName].status = doNotCopy
				}
			}
		}
	}
	tarExpectedSize := int64(0)
	tarBall := c.tarBallQueue.Deque()
	for fileName := range(c.flieInfos) {
		file := c.flieInfos[fileName]
		fileSize := file.info.fileInfo.Size()
		if file.status == possibleCopy {
			c.copyTar(c.prevFileTar[fileName])
		} else if tarExpectedSize + fileSize > c.tarBallQueue.TarSizeThreshold {			
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
	return c.tarFileSets, nil
}

func (c *CopyTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
