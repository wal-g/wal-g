package extract

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"strings"
)

type S3ReaderMaker struct {
	Prefix *Prefix
	Path   string
}

type Prefix struct {
	Creds  *credentials.Credentials
	Bucket *string
	Path   *string
	Svc    *s3.S3
}

type Backup struct {
	p    *Prefix
	name string
}

func StripName(key string) string {
	all := strings.SplitAfter(key, "/")
	name := strings.Split(all[2], "_backup")[0]
	return name
}

func (s *S3ReaderMaker) Reader() io.ReadCloser {
	input := &s3.GetObjectInput{
		Bucket: s.Prefix.Bucket,
		Key:    aws.String(s.Path),
	}
	fmt.Println(input)
	rdr, err := s.Prefix.Svc.GetObject(input)
	if err != nil {
		panic(err)
	}
	return rdr.Body

}

func GetPaths(p Prefix, path string) []string {
	objects := &s3.ListObjectsInput{
		Bucket:    p.Bucket,
		Prefix:    aws.String(path),
		Delimiter: aws.String(".txt"),
	}

	files, err := p.Svc.ListObjects(objects)
	if err != nil {
		panic(err)
	}

	arr := make([]string, len(files.Contents))

	for i, ob := range files.Contents {
		key := *ob.Key

		arr[i] = key
	}
	return arr
}
