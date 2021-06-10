package postgres

import (
	"archive/tar"
	"os"
)

type CopyTarBallComposer struct {
	changedTarBalls map[string]bool
	fileTarSets map[string]string
}

type CopyTarBallComposerMaker struct {
	previousBackup Backup
}

func NewCopyTarBallComposerMaker(previousBackup Backup) *CopyTarBallComposerMaker {
	return &CopyTarBallComposerMaker{previousBackup}
}

func (maker *CopyTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	var composer CopyTarBallComposer
	for tarName, fileSet := range maker.previousBackup.SentinelDto.TarFileSets {
		for _, fileName := range fileSet {
			composer.fileTarSets[fileName] = tarName
		}
		composer.changedTarBalls[tarName] = false
	}
	return &composer, nil
}

func (c *CopyTarBallComposer) AddFile(info *ComposeFileInfo) {

}

func (c *CopyTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	return nil
}

func (c *CopyTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {

}

func (c *CopyTarBallComposer) PackTarballs() (TarFileSets, error) {
	return make(TarFileSets), nil
}

func (c *CopyTarBallComposer) GetFiles() BundleFiles {
	return &RegularBundleFiles{}
}
