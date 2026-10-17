package greenplum

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewIncrementalTarInterpreter(dbDataDirectory string, sentinel postgres.BackupSentinelDto, filesMetadata postgres.FilesMetadataDto,
	aoFilesMetadata ao.FilesMetadataDTO,
	filesToUnwrap map[string]bool, createNewIncrementalFiles bool, allowDataLoss bool) *IncrementalTarInterpreter {
	return &IncrementalTarInterpreter{
		FileTarInterpreter: postgres.NewFileTarInterpreter(dbDataDirectory, sentinel, filesMetadata, filesToUnwrap, createNewIncrementalFiles),
		fsync:              !viper.GetBool(conf.TarDisableFsyncSetting),
		aoFilesMetadata:    aoFilesMetadata,
		allowDataLoss:      allowDataLoss,
	}
}

type IncrementalTarInterpreter struct {
	*postgres.FileTarInterpreter
	fsync           bool
	aoFilesMetadata ao.FilesMetadataDTO
	allowDataLoss   bool
}

func (i *IncrementalTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	aoMeta, ok := i.aoFilesMetadata.Files[header.Name]
	if !ok {
		return i.FileTarInterpreter.Interpret(reader, header)
	}
	if aoMeta.IsIncremented {
		return ao.ApplyFileIncrement(path.Join(i.DBDataDirectory, header.Name), reader, i.fsync)
	}
	if i.allowDataLoss {
		// write new full AO files as temporary file... So, we will be able to recovery it if
		// extraction failed.
		// Note: regular extraction removes corrupted files
		return i.writeAOFile(reader, header, aoMeta)
	}
	return i.FileTarInterpreter.Interpret(reader, header)
}

func (i *IncrementalTarInterpreter) writeAOFile(reader io.Reader, header *tar.Header, meta *ao.BackupFileDesc) error {
	targetPath := path.Join(i.DBDataDirectory, header.Name)
	if err := postgres.PrepareDirs(header.Name, targetPath); err != nil {
		return err
	}
	partialPath := targetPath + ".wal-g-partial"
	file, err := os.OpenFile(partialPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	copied, copyErr := io.Copy(file, reader)
	if copyErr == nil && copied != meta.EOF {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr == nil {
		copyErr = file.Chmod(os.FileMode(meta.FileMode))
	}
	if copyErr == nil && i.fsync {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(partialPath, targetPath)
}

// RecoverFailedFiles is called only after ExtractAll exhausted its retries.
func (i *IncrementalTarInterpreter) RecoverFailedFiles(_ context.Context, files []internal.ReaderMaker) ([]internal.ReaderMaker, error) {
	if !i.allowDataLoss {
		return files, nil
	}
	unrecovered := make([]internal.ReaderMaker, 0)
	for _, file := range files {
		meta, ok := i.aoFilesMetadata.Files[file.LocalPath()]
		if !ok {
			unrecovered = append(unrecovered, file)
			continue
		}
		if err := i.recoverAOFile(file.LocalPath(), meta); err != nil {
			return nil, fmt.Errorf("recover AO file %s: %w", file.LocalPath(), err)
		}
	}
	return unrecovered, nil
}

func (i *IncrementalTarInterpreter) recoverAOFile(fileName string, meta *ao.BackupFileDesc) error {
	targetPath := path.Join(i.DBDataDirectory, fileName)
	if err := postgres.PrepareDirs(fileName, targetPath); err != nil {
		return err
	}
	if meta.EOF < 0 {
		return fmt.Errorf("invalid AO EOF %d", meta.EOF)
	}
	if !meta.IsIncremented {
		// when --allow-dataloss passed we firstly write new AO-file to new partial file
		// If write fails due to some error - we will have partial file on a disk
		// move it to proper place and then bzero to match EOF
		partialPath := targetPath + ".wal-g-partial"
		if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Rename(partialPath, targetPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := ao.ZeroFillFileToEOF(targetPath, meta.EOF, os.FileMode(meta.FileMode), i.fsync); err != nil {
		return err
	}
	tracelog.WarningLogger.Printf("AO data loss: recovered %s with zero-filled missing bytes", fileName)
	return nil
}
