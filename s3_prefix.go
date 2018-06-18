package walg

import "github.com/aws/aws-sdk-go/service/s3/s3iface"

// S3Prefix contains the S3 service client, bucket and string.
type S3Prefix struct {
	Svc    s3iface.S3API
	Bucket *string
	Server *string
}