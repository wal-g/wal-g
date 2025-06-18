package internal

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

const TarPartitionFolderName = "/tar_partitions/"

// StorageTarBall represents a tar file that is
// going to be uploaded to storage.
type StorageTarBall struct {
	backupName  string
	partNumber  int
	partSize    *int64
	writeCloser io.Closer
	tarWriter   *tar.Writer
	uploader    Uploader
	name        string
}

func (tarBall *StorageTarBall) Name() string {
	return tarBall.name
}

// SetUp creates a new tar writer and starts upload to storage.
// Upload will block until the tar file is finished writing.
// If a name for the file is not given, default name is of
// the form `part_....tar.[Compressor file extension]`.
func (tarBall *StorageTarBall) SetUp(ctx context.Context, crypter crypto.Crypter, names ...string) error {
	if tarBall.tarWriter == nil {
		if len(names) > 0 {
			tarBall.name = names[0]
		} else {
			tarBall.name = fmt.Sprintf("part_%0.3d.tar.%v", tarBall.partNumber, tarBall.uploader.Compression().FileExtension())
		}
		writeCloser, err := tarBall.startUpload(ctx, tarBall.name, crypter)
		if err != nil {
			return err
		}

		tarBall.writeCloser = writeCloser
		tarBall.tarWriter = tar.NewWriter(writeCloser)
	}
	return nil
}

// CloseTar closes the tar writer, flushing any unwritten data
// to the underlying writer before also closing the underlying writer.
func (tarBall *StorageTarBall) CloseTar() error {
	err := tarBall.tarWriter.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close tar writer")
	}

	err = tarBall.writeCloser.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close underlying writer")
	}
	tracelog.InfoLogger.Printf("Finished writing part %d.\n", tarBall.partNumber)
	return nil
}

func (tarBall *StorageTarBall) AwaitUploads() error {
	if tarBall.uploader.Finish() != nil {
		return fmt.Errorf("Unable to complete uploads")
	}
	return nil
}

func GetBackupTarPath(backupName, fileName string) string {
	return backupName + TarPartitionFolderName + fileName
}

// TODO : unit tests
// startUpload creates a compressing writer and runs upload in the background once
// a compressed tar member is finished writing.
func (tarBall *StorageTarBall) startUpload(ctx context.Context, name string, crypter crypto.Crypter) (io.WriteCloser, error) {
	pipeReader, pipeWriter := io.Pipe()
	uploader := tarBall.uploader

	path := GetBackupTarPath(tarBall.backupName, name)

	tracelog.InfoLogger.Printf("Starting part %d ...\n", tarBall.partNumber)

	go func() {
		err := uploader.Upload(ctx, path, pipeReader)
		if err != nil {
			if compressingError, ok := err.(CompressAndEncryptError); ok {
				tracelog.ErrorLogger.Printf("could not upload '%s' due to compression error\n%+v\n", path, compressingError)
			}
			tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", path)
			tracelog.ErrorLogger.Printf("%v\n", err)
			if err = pipeReader.Close(); err != nil {
				tracelog.ErrorLogger.Printf("Failed to close pipe: %v\n", err)
			}
			tracelog.ErrorLogger.Printf(
				"Unable to continue the backup process because of the loss of a part %d.\n",
				tarBall.partNumber)
		}
	}()

	var writerToCompress io.WriteCloser = pipeWriter

	if crypter != nil {
		encryptedWriter, err := crypter.Encrypt(pipeWriter)

		if err != nil {
			return nil, errors.Wrap(err, "upload: encryption error")
		}

		writerToCompress = &utility.CascadeWriteCloser{WriteCloser: encryptedWriter, Underlying: pipeWriter}
	}

	return &utility.CascadeWriteCloser{WriteCloser: uploader.Compression().NewWriter(writerToCompress),
		Underlying: writerToCompress}, nil
}

// Size accumulated in this tarball
func (tarBall *StorageTarBall) Size() int64 { return atomic.LoadInt64(tarBall.partSize) }

// AddSize to total Size
func (tarBall *StorageTarBall) AddSize(i int64) { atomic.AddInt64(tarBall.partSize, i) }

func (tarBall *StorageTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
