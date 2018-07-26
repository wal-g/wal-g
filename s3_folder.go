package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Folder struct {
	S3API  s3iface.S3API
	Bucket *string
	Server *string
}

func NewS3Folder(s3API s3iface.S3API, bucket, server string) *S3Folder {
	return &S3Folder{
		s3API,
		aws.String(bucket),
		aws.String(server),
	}
}
