package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"io"
)

// S3ReaderMaker creates readers for downloading from S3
type S3ReaderMaker struct {
	Folder       *S3Folder
	RelativePath string
}

func NewS3ReaderMaker(folder *S3Folder, key string) *S3ReaderMaker {
	return &S3ReaderMaker{folder, key}
}

func (readerMaker *S3ReaderMaker) Path() string { return readerMaker.RelativePath }

// Reader creates a new S3 reader for each S3 object.
func (readerMaker *S3ReaderMaker) Reader() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: readerMaker.Folder.Bucket,
		Key:    aws.String(readerMaker.RelativePath),
	}

	rdr, err := readerMaker.Folder.S3API.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "S3 Reader: s3.GetObject failed")
	}
	return rdr.Body, nil

}
