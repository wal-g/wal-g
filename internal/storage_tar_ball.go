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
func (tarBall *StorageTarBall) SetUp(crypter crypto.Crypter, names ...string) {
	if tarBall.tarWriter == nil {
		if len(names) > 0 {
			tarBall.name = names[0]
		} else {
			tarBall.name = fmt.Sprintf("part_%0.3d.tar.%v", tarBall.partNumber, tarBall.uploader.Compression().FileExtension())
		}
		writeCloser := tarBall.startUpload(tarBall.name, crypter)

		tarBall.writeCloser = writeCloser
		tarBall.tarWriter = tar.NewWriter(writeCloser)
	}
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

func (tarBall *StorageTarBall) AwaitUploads() {
	tarBall.uploader.Finish()
	if tarBall.uploader.Failed() {
		tracelog.ErrorLogger.Fatal("Unable to complete uploads")
	}
}

// TODO : unit tests
// startUpload creates a compressing writer and runs upload in the background once
// a compressed tar member is finished writing.
func (tarBall *StorageTarBall) startUpload(name string, crypter crypto.Crypter) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()
	uploader := tarBall.uploader

	path := tarBall.backupName + TarPartitionFolderName + name

	tracelog.InfoLogger.Printf("Starting part %d ...\n", tarBall.partNumber)

	go func() {
		err := uploader.Upload(context.Background(), path, pipeReader)
		if compressingError, ok := err.(CompressAndEncryptError); ok {
			tracelog.ErrorLogger.Printf("could not upload '%s' due to compression error\n%+v\n", path, compressingError)
		}
		if err != nil {
			tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", path)
			tracelog.ErrorLogger.Printf("%v\n", err)
			err = pipeReader.Close()
			tracelog.ErrorLogger.FatalfOnError("Failed to close pipe: %v", err)
			tracelog.ErrorLogger.Fatalf(
				"Unable to continue the backup process because of the loss of a part %d.\n",
				tarBall.partNumber)
		}
	}()

	var writerToCompress io.WriteCloser = pipeWriter

	if crypter != nil {
		encryptedWriter, err := crypter.Encrypt(pipeWriter)

		if err != nil {
			tracelog.ErrorLogger.Fatal("upload: encryption error ", err)
		}

		writerToCompress = &utility.CascadeWriteCloser{WriteCloser: encryptedWriter, Underlying: pipeWriter}
	}

	return &utility.CascadeWriteCloser{WriteCloser: uploader.Compression().NewWriter(writerToCompress),
		Underlying: writerToCompress}
}

// Size accumulated in this tarball
func (tarBall *StorageTarBall) Size() int64 { return atomic.LoadInt64(tarBall.partSize) }

// AddSize to total Size
func (tarBall *StorageTarBall) AddSize(i int64) { atomic.AddInt64(tarBall.partSize, i) }

func (tarBall *StorageTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
