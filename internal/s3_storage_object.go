package internal

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"time"
)

type S3StorageObject struct {
	*s3.Object
}

func NewS3StorageObject(object *s3.Object) *S3StorageObject {
	return &S3StorageObject{object}
}

func (object *S3StorageObject) GetAbsPath() string {
	return *object.Key
}

func (object *S3StorageObject) GetLastModified() time.Time {
	return *object.LastModified
}
