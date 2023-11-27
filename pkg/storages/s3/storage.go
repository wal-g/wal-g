package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

type Config struct {
	Secrets                  *Secrets `json:"-"`
	Region                   string
	Endpoint                 string
	EndpointSource           string
	EndpointPort             string
	Bucket                   string
	RootPath                 string
	AccessKey                string
	SessionToken             string
	RoleARN                  string
	SessionName              string
	CACertFile               string
	UseYCSessionToken        string
	ForcePathStyle           bool
	RequestAdditionalHeaders string
	UseListObjectsV1         bool
	MaxRetries               int
	LogLevel                 string
	Uploader                 *UploaderConfig
	RangeBatchEnabled        bool
	RangeMaxRetries          int
}

type Secrets struct {
	SecretKey string
}

// TODO: Unit tests
func NewStorage(config *Config) (*Storage, error) {
	sess, err := createSession(config)
	if err != nil {
		return nil, fmt.Errorf("create new AWS session: %w", err)
	}

	s3Client := s3.New(sess)

	uploader, err := createUploader(s3Client, config.Uploader)
	if err != nil {
		return nil, fmt.Errorf("create new S3 uploader: %w", err)
	}

	folder := NewFolder(s3Client, uploader, config.RootPath, config)

	hash, err := storage.ComputeConfigHash("s3", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{folder, hash}, nil
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) SetRootFolder(folder storage.Folder) {
	s.rootFolder = folder
}

func (s *Storage) ConfigHash() string {
	return s.hash
}

func (s *Storage) Close() error {
	// Nothing to close: the S3 session doesn't require to be closed
	return nil
}
