package walg

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const DefaultStreamingPartSizeFor10Concurrency = 20 << 20

// MaxRetries limit upload and download retries during interaction with S3
var MaxRetries = 15

// Given an S3 bucket name, attempt to determine its region
func findS3BucketRegion(bucket string, config *aws.Config) (string, error) {
	input := s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		return "", err
	}

	output, err := s3.New(sess).GetBucketLocation(&input)
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		// buckets in "US Standard", a.k.a. us-east-1, are returned as a nil region
		return "us-east-1", nil
	}
	// all other regions are strings
	return *output.LocationConstraint, nil
}

// Configure connects to S3 and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
//
// Requires these environment variables to be set:
// WALE_S3_PREFIX
//
// Able to configure the upload part size in the S3 uploader.
func Configure() (*Uploader, *S3Folder, error) { // TODO : add parameter naming
	waleS3Prefix := os.Getenv("WALE_S3_PREFIX")
	if waleS3Prefix == "" {
		return nil, nil, &UnsetEnvVarError{names: []string{"WALE_S3_PREFIX"}}
	}

	waleS3Url, err := url.Parse(waleS3Prefix)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Configure: failed to parse url '%s'", waleS3Prefix)
	}
	if waleS3Url.Scheme == "" || waleS3Url.Host == "" {
		return nil, nil, fmt.Errorf("Missing url scheme=%q and/or host=%q", waleS3Url.Scheme, waleS3Url.Host)
	}

	bucket := waleS3Url.Host
	server := strings.TrimPrefix(waleS3Url.Path, "/")

	// Allover the code this parameter is concatenated with '/'.
	// TODO: Get rid of numerous string literals concatenated with this
	server = strings.TrimSuffix(server, "/")

	config := defaults.Get().Config

	config.MaxRetries = &MaxRetries
	if _, err := config.Credentials.Get(); err != nil {
		return nil, nil, errors.Wrapf(err, "Configure: failed to get AWS credentials; please specify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}

	s3ForcePathStyleStr := os.Getenv("AWS_S3_FORCE_PATH_STYLE")
	if len(s3ForcePathStyleStr) > 0 {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Configure: failed parse AWS_S3_FORCE_PATH_STYLE")
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region, err = findS3BucketRegion(bucket, config)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "Configure: AWS_REGION is not set and s3:GetBucketLocation failed")
		}
	}
	config = config.WithRegion(region)

	compressionMethod := os.Getenv("WALG_COMPRESSION_METHOD")
	if compressionMethod == "" {
		compressionMethod = Lz4AlgorithmName
	}
	if _, ok := Compressors[compressionMethod]; !ok {
		return nil, nil, UnknownCompressionMethodError{}
	}

	diskLimitStr := os.Getenv("WALG_DISK_RATE_LIMIT")
	if diskLimitStr != "" {
		diskLimit, err := strconv.ParseInt(diskLimitStr, 10, 64)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Configure: failed to parse WALG_DISK_RATE_LIMIT")
		}
		DiskLimiter = rate.NewLimiter(rate.Limit(diskLimit), int(diskLimit+64*1024)) // Add 8 pages to possible bursts
	}

	netLimitStr := os.Getenv("WALG_NETWORK_RATE_LIMIT")
	if netLimitStr != "" {
		netLimit, err := strconv.ParseInt(netLimitStr, 10, 64)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Configure: failed to parse WALG_NETWORK_RATE_LIMIT")
		}
		NetworkLimiter = rate.NewLimiter(rate.Limit(netLimit), int(netLimit+64*1024)) // Add 8 pages to possible bursts
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Configure: failed to create new session")
	}

	useWalDeltaStr, hasUseWalDelta := os.LookupEnv("WALG_USE_WAL_DELTA")
	useWalDelta := false
	if hasUseWalDelta {
		useWalDelta, err = strconv.ParseBool(useWalDeltaStr)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "Cofigure: failed to parse WALG_USE_WAL_DELTA")
		}
	}

	folder := NewS3Folder(s3.New(sess), bucket, server)

	var concurrency = getMaxUploadConcurrency(10)
	uploaderApi := createUploader(folder.S3API, DefaultStreamingPartSizeFor10Concurrency, concurrency)
	uploader := NewUploader(uploaderApi, Compressors[compressionMethod], folder, useWalDelta)

	storageClass, ok := os.LookupEnv("WALG_S3_STORAGE_CLASS")
	if ok {
		uploader.StorageClass = storageClass
	}

	serverSideEncryption, ok := os.LookupEnv("WALG_S3_SSE")
	if ok {
		uploader.serverSideEncryption = serverSideEncryption
	}

	sseKmsKeyId, ok := os.LookupEnv("WALG_S3_SSE_KMS_ID")
	if ok {
		uploader.SSEKMSKeyId = sseKmsKeyId
	}

	// Only aws:kms implies sseKmsKeyId
	if (serverSideEncryption == "aws:kms") == (sseKmsKeyId == "") {
		return nil, nil, errors.New("Configure: WALG_S3_SSE_KMS_ID must be set iff using aws:kms encryption")
	}

	return uploader, folder, err
}

// createUploader returns an uploader with customizable concurrency
// and partsize.
func createUploader(svc s3iface.S3API, partsize, concurrency int) s3manageriface.UploaderAPI {
	uploaderAPI := s3manager.NewUploaderWithClient(svc, func(uploader *s3manager.Uploader) {
		uploader.PartSize = int64(partsize)
		uploader.Concurrency = concurrency
	})
	return uploaderAPI
}
