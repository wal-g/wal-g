package postgres

import (
	"archive/tar"
	"io"
	"os"
	"path"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

type FileInterpretFinishedHandler interface {
	OnInterpretationFinished(header *tar.Header) error
}

type InterpretFinishedHandler interface {
	OnInterpretationFinished() error
}

type GroupFileTarInterpreter struct {
	FileTarInterpreter
	FileInterpretFinishedHandlers []FileInterpretFinishedHandler
	InterpretFinishedHandlers     []InterpretFinishedHandler
}

func (groupTarInterpreter *GroupFileTarInterpreter) InterpretGroup(tarReader *tar.Reader) error {
	group := new(errgroup.Group)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "GroupTarInterpreter: tar extract failed")
		}

		err = groupTarInterpreter.Interpret(tarReader, header)
		if err != nil {
			return errors.Wrap(err, "GroupTarInterpreter: Interpret failed")
		}

		for _, handler := range groupTarInterpreter.FileInterpretFinishedHandlers {
			closure := handler
			group.Go(func() error { return closure.OnInterpretationFinished(header) })
		}
	}

	err := group.Wait()
	if err != nil {
		return errors.Wrap(err, "GroupTarInterpreter: InterpretGroup failed")
	}

	for _, handler := range groupTarInterpreter.InterpretFinishedHandlers {
		err := handler.OnInterpretationFinished()
		if err != nil {
			return err
		}
	}

	return nil
}

type FileSyncHandler struct {
	BasePath string
}

func (handler FileSyncHandler) OnInterpretationFinished(header *tar.Header) error {
	targetPath := path.Join(handler.BasePath, header.Name)
	file, err := os.OpenFile(targetPath, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to open file for fsync: '%s'", targetPath)
	}
	err = file.Sync()
	return errors.Wrap(err, "FileSyncHandler: fsync failed")
}

type GlobalFileSyncHandler struct {
}

func (handler GlobalFileSyncHandler) OnInterpretationFinished() error {
	_, _, err := unix.Syscall(unix.SYS_SYNC, 0, 0, 0)
	if err != 0 {
		return errors.Errorf("GlobalFileSyncHandler: global fsync failed with error code %d", err)
	}
	return nil
}
