package walg

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"io"
)

// S3ReaderMaker handles cases where backups need to be uploaded to
// S3.
type S3ReaderMaker struct {
	Backup     *Backup
	Key        *string
	FileFormat string
}

func (readerMaker *S3ReaderMaker) Format() string { return readerMaker.FileFormat }

// Path to file in bucket
func (readerMaker *S3ReaderMaker) Path() string { return *readerMaker.Key }

// Reader creates a new S3 reader for each S3 object.
func (readerMaker *S3ReaderMaker) Reader() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: readerMaker.Backup.Folder.Bucket,
		Key:    readerMaker.Key,
	}

	rdr, err := readerMaker.Backup.Folder.S3API.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "S3 Reader: s3.GetObject failed")
	}
	return rdr.Body, nil

}
