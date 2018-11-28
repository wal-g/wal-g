package internal

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"golang.org/x/time/rate"
	"os"
	"path/filepath"
)

const (
	DefaultStreamingPartSizeFor10Concurrency = 20 << 20
	DefaultDataBurstRateLimit                = 8 * int64(DatabasePageSize)
	DefaultDataFolderPath                    = "/tmp"
)

type SseKmsIdNotSetError struct {
	error
}

func NewSseKmsIdNotSetError() SseKmsIdNotSetError {
	return SseKmsIdNotSetError{errors.New("Configure: WALG_S3_SSE_KMS_ID must be set if using aws:kms encryption")}
}

func (err SseKmsIdNotSetError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// MaxRetries limit upload and download retries during interaction with S3
var MaxRetries = 15

// TODO : unit tests
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

// TODO : unit tests
func getS3Path() (bucket, server string, err error) {
	waleS3Prefix := getSettingValue("WALE_S3_PREFIX")
	if waleS3Prefix == "" {
		return "", "", NewUnsetEnvVarError([]string{"WALE_S3_PREFIX"})
	}

	waleS3Url, err := url.Parse(waleS3Prefix)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to parse url '%s'", waleS3Prefix)
	}
	if waleS3Url.Scheme == "" || waleS3Url.Host == "" {
		return "", "", errors.Errorf("missing url scheme=%q and/or host=%q", waleS3Url.Scheme, waleS3Url.Host)
	}

	bucket = waleS3Url.Host
	server = strings.TrimPrefix(waleS3Url.Path, "/")

	// Allover the code this parameter is concatenated with '/'.
	// TODO: Get rid of numerous string literals concatenated with this
	server = strings.TrimSuffix(server, "/")
	return bucket, server, nil
}

// TODO : unit tests
func configureLimiters() error {
	if diskLimitStr := getSettingValue("WALG_DISK_RATE_LIMIT"); diskLimitStr != "" {
		diskLimit, err := strconv.ParseInt(diskLimitStr, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse WALG_DISK_RATE_LIMIT")
		}
		DiskLimiter = rate.NewLimiter(rate.Limit(diskLimit), int(diskLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}

	if netLimitStr := getSettingValue("WALG_NETWORK_RATE_LIMIT"); netLimitStr != "" {
		netLimit, err := strconv.ParseInt(netLimitStr, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse WALG_NETWORK_RATE_LIMIT")
		}
		NetworkLimiter = rate.NewLimiter(rate.Limit(netLimit), int(netLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}
	return nil
}

// TODO : unit tests
func getAWSRegion(s3Bucket string, config *aws.Config) (string, error) {
	region := getSettingValue("AWS_REGION")
	if region == "" {
		if config.Endpoint == nil ||
			*config.Endpoint == "" ||
			strings.HasSuffix(*config.Endpoint, ".amazonaws.com") {
			var err error
			region, err = findS3BucketRegion(s3Bucket, config)
			if err != nil {
				return "", errors.Wrapf(err, "AWS_REGION is not set and s3:GetBucketLocation failed")
			}
		} else {
			// For S3 compatible services like Minio, Ceph etc. use `us-east-1` as region
			// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
			region = "us-east-1"
		}
	}
	return region, nil
}

//TODO : unit tests
func createS3Session(s3Bucket string) (*session.Session, error) {
	config := defaults.Get().Config

	config.MaxRetries = &MaxRetries
	if _, err := config.Credentials.Get(); err != nil {
		return nil, errors.Wrapf(err, "failed to get AWS credentials; please specify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	if endpoint := getSettingValue("AWS_ENDPOINT"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}

	if s3ForcePathStyleStr := getSettingValue("AWS_S3_FORCE_PATH_STYLE"); s3ForcePathStyleStr != "" {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse AWS_S3_FORCE_PATH_STYLE")
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region, err := getAWSRegion(s3Bucket, config)
	if err != nil {
		return nil, err
	}
	config = config.WithRegion(region)

	return session.NewSession(config)
}

// TODO : unit tests
func configureS3Uploader(s3Client *s3.S3) (*S3Uploader, error) {
	var concurrency = getMaxUploadConcurrency(10)
	uploaderApi := CreateUploaderAPI(s3Client, DefaultStreamingPartSizeFor10Concurrency, concurrency)

	serverSideEncryption, sseKmsKeyId, err := configureServerSideEncryption()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure server side encryption")
	}

	storageClass, ok := LookupConfigValue("WALG_S3_STORAGE_CLASS")
	if !ok {
		storageClass = "STANDARD"
	}
	return NewS3Uploader(uploaderApi, serverSideEncryption, sseKmsKeyId, storageClass), nil
}

// TODO : unit tests
func configureS3Folder() (*S3Folder, error) {
	s3Bucket, s3Path, err := getS3Path()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 path")
	}
	sess, err := createS3Session(s3Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new session")
	}
	s3Client := s3.New(sess)
	s3Uploader, err := configureS3Uploader(s3Client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 uploader")
	}
	folder := NewS3Folder(*s3Uploader, s3Client, s3Bucket, s3Path)
	return folder, nil
}

// TODO : unit tests
func getDataFolderPath() string {
	pgdata, ok := LookupConfigValue("PGDATA")
	var dataFolderPath string
	if !ok {
		dataFolderPath = DefaultDataFolderPath
	} else {
		dataFolderPath = filepath.Join(pgdata, "pg_wal")
		if _, err := os.Stat(dataFolderPath); err != nil {
			dataFolderPath = filepath.Join(pgdata, "pg_xlog")
			if _, err := os.Stat(dataFolderPath); err != nil {
				dataFolderPath = DefaultDataFolderPath
			}
		}
	}
	dataFolderPath = filepath.Join(dataFolderPath, "walg_data")
	return dataFolderPath
}

// TODO : unit tests
func configureWalDeltaUsage() (useWalDelta bool, deltaDataFolder DataFolder, err error) {
	if useWalDeltaStr, ok := LookupConfigValue("WALG_USE_WAL_DELTA"); ok {
		useWalDelta, err = strconv.ParseBool(useWalDeltaStr)
		if err != nil {
			return false, nil, errors.Wrapf(err, "failed to parse WALG_USE_WAL_DELTA")
		}
	}
	if !useWalDelta {
		return
	}
	dataFolderPath := getDataFolderPath()
	deltaDataFolder, err = NewDiskDataFolder(dataFolderPath)
	if err != nil {
		useWalDelta = false
		tracelog.WarningLogger.Printf("can't use wal delta feature because can't open delta data folder '%s'"+
			" due to error: '%v'\n", dataFolderPath, err)
		err = nil
	}
	return
}

// TODO : unit tests
func configureCompressor() (Compressor, error) {
	compressionMethod := getSettingValue("WALG_COMPRESSION_METHOD")
	if compressionMethod == "" {
		compressionMethod = Lz4AlgorithmName
	}
	if _, ok := Compressors[compressionMethod]; !ok {
		return nil, NewUnknownCompressionMethodError()
	}
	return Compressors[compressionMethod], nil
}

// TODO : unit tests
func configureServerSideEncryption() (serverSideEncryption string, sseKmsKeyId string, err error) {
	serverSideEncryption, _ = LookupConfigValue("WALG_S3_SSE")
	sseKmsKeyId, _ = LookupConfigValue("WALG_S3_SSE_KMS_ID")

	// Only aws:kms implies sseKmsKeyId
	if (serverSideEncryption == "aws:kms") == (sseKmsKeyId == "") {
		return "", "", NewSseKmsIdNotSetError()
	}
	return
}

// TODO : unit tests
func configureLogging() error {
	logLevel, ok := LookupConfigValue("WALG_LOG_LEVEL")
	if ok {
		return tracelog.UpdateLogLevel(logLevel)
	}
	return nil
}

// Configure connects to S3 and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
//
// Requires these environment variables to be set:
// WALE_S3_PREFIX
//
// Able to configure the upload part size in the S3 uploader.
func Configure() (uploader *Uploader, destinationFolder StorageFolder, err error) {
	err = configureLogging()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure logging")
	}

	err = configureLimiters()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure limiters")
	}

	folder, err := configureS3Folder()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure S3 folder")
	}

	compressor, err := configureCompressor()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure compression")
	}

	useWalDelta, deltaDataFolder, err := configureWalDeltaUsage()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure WAL Delta usage")
	}

	preventWalOverwrite := false
	if preventWalOverwriteStr := getSettingValue("WALG_PREVENT_WAL_OVERWRITE"); preventWalOverwriteStr != "" {
		preventWalOverwrite, err = strconv.ParseBool(preventWalOverwriteStr)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse WALG_PREVENT_WAL_OVERWRITE")
		}
	}

	uploader = NewUploader(compressor, folder, deltaDataFolder, useWalDelta, preventWalOverwrite)

	return uploader, folder, err
}

// CreateUploaderAPI returns an uploader with customizable concurrency
// and part size.
func CreateUploaderAPI(svc s3iface.S3API, partsize, concurrency int) s3manageriface.UploaderAPI {
	uploaderAPI := s3manager.NewUploaderWithClient(svc, func(uploader *s3manager.Uploader) {
		uploader.PartSize = int64(partsize)
		uploader.Concurrency = concurrency
	})
	return uploaderAPI
}
