package helpers

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	S3PORT = 9000
)

func ObjectKeyFromS3Prefix(s3Prefix, expectedBucket string, elements ...string) (string, error) {
	prefix, err := url.Parse(s3Prefix)
	if err != nil {
		return "", fmt.Errorf("parse S3 prefix: %w", err)
	}
	if prefix.Scheme != "s3" || prefix.Host == "" {
		return "", fmt.Errorf("invalid S3 prefix %q", s3Prefix)
	}
	if prefix.Host != expectedBucket {
		return "", fmt.Errorf("S3 prefix bucket %q does not match configured bucket %q", prefix.Host, expectedBucket)
	}
	if prefix.RawQuery != "" || prefix.Fragment != "" {
		return "", fmt.Errorf("S3 prefix must not contain a query or fragment: %q", s3Prefix)
	}

	keyElements := []string{strings.Trim(prefix.Path, "/")}
	keyElements = append(keyElements, elements...)
	return strings.TrimPrefix(path.Join(keyElements...), "/"), nil
}

type S3Client struct {
	s3         *s3.Client
	downloader *manager.Downloader
	bucket     string
}

// s3ClientConfig collects the v2-style settings the test S3 client needs.
// In v1 these all lived on session.Session/aws.Config; in v2 endpoint and
// path-style settings move to per-client functional options.
type s3ClientConfig struct {
	accessKey string
	secretKey string
	endpoint  string
	region    string
}

func NewS3Client(cfg s3ClientConfig, bucket string) (*S3Client, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.accessKey, cfg.secretKey, "")),
		config.WithRegion(cfg.region),
	)
	if err != nil {
		return nil, err
	}

	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		if cfg.endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.endpoint)
		}
	})

	return &S3Client{s3: cli, downloader: manager.NewDownloader(cli), bucket: bucket}, nil
}

func (cl *S3Client) FileContents(key string) ([]byte, error) {
	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := cl.downloader.Download(context.Background(), buf,
		&s3.GetObjectInput{
			Key:    aws.String(key),
			Bucket: aws.String(cl.bucket),
		})
	if err != nil {
		return nil, fmt.Errorf("unable to download item %q, %v", key, err)
	}
	return buf.Bytes(), err
}

func (cl *S3Client) List(ctx context.Context, path string) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(cl.s3, &s3.ListObjectsV2Input{
		Bucket:    aws.String(cl.bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	})
	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing bucket: %w", err)
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	return keys, nil
}

func (cl *S3Client) PutEmptyObject(key string) error {
	_, err := cl.s3.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(cl.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(nil),
	})
	if err != nil {
		return fmt.Errorf("unable to put empty object %q: %w", key, err)
	}
	return nil
}

func (cl *S3Client) RequireObject(key string) error {
	_, err := cl.s3.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(cl.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("expected object %q to exist: %w", key, err)
	}
	return nil
}

type S3Storage struct {
	ctx    context.Context
	host   string
	bucket string
	access string
	secret string
	client *S3Client
}

func NewS3Storage(ctx context.Context, host, bucket, access, secret string) *S3Storage {
	return &S3Storage{ctx: ctx, host: host, bucket: bucket, access: access, secret: secret}
}

func (s *S3Storage) Client() (*S3Client, error) {
	if s.client == nil {
		s3Host, err := DockerContainer(s.ctx, s.host)
		if err != nil {
			return nil, err
		}

		host, port, err := ExposedPort(*s3Host, S3PORT)
		if err != nil {
			return nil, err
		}
		cfg := s3ClientConfig{
			accessKey: s.access,
			secretKey: s.secret,
			endpoint:  fmt.Sprintf("http://%s:%d", host, port),
			region:    "test-region",
		}

		client, err := NewS3Client(cfg, s.bucket)
		if err != nil {
			return nil, err
		}
		s.client = client
	}

	return s.client, nil
}

func (s *S3Storage) WaitForBucket() error {
	var lastErr error
	for i := 0; i < 100; i++ {
		client, err := s.Client()
		if err == nil {
			_, err = client.s3.HeadBucket(s.ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
			if err == nil {
				return nil
			}
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("S3 bucket %q is not available: %w", s.bucket, lastErr)
}

func (s *S3Storage) Archives() ([]Archive, error) {
	cl, err := s.Client()
	if err != nil {
		return nil, err
	}

	// TODO: remove hardcoded path
	keys, err := cl.List(s.ctx, "mongodb-backup/test_uuid/test_mongodb/oplog_005/")
	if err != nil {
		return nil, err
	}

	var archives []Archive
	for _, arch := range keys {
		reArch, _ := regexp.Compile(`oplog_(\d+\.\d+)_(\d+\.\d+)\.`)
		timestamps := reArch.FindAllStringSubmatch(arch, -1)
		for i := range timestamps {
			startTS, startErr := TimestampFromStr(timestamps[i][1])
			endTS, endErr := TimestampFromStr(timestamps[i][2])
			if startErr != nil || endErr != nil {
				return nil, fmt.Errorf("wrong archive name format: %v, %v", startErr, endErr)
			}
			archives = append(archives, Archive{startTS, endTS})
		}
	}
	return archives, nil
}

func (s *S3Storage) ArchTsExists(ts OpTimestamp) (bool, error) {
	archives, err := s.Archives()
	if err != nil {
		return false, err
	}
	if slices.ContainsFunc(archives, func(a Archive) bool { return archiveContainsTS(a, ts) }) {
		return true, nil
	}

	return false, fmt.Errorf("archive with ts '%v' was not found", ts)
}

func archiveContainsTS(archive Archive, ts OpTimestamp) bool {
	return LessTS(archive.StartTS, ts) && !LessTS(archive.EndTS, ts)
}
