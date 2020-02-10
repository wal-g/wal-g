package helpers

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/wal-g/tracelog"
)

const (
	S3PORT = 9000
)

type S3Client struct {
	sess       *session.Session
	s3         *s3.S3
	downloader *s3manager.Downloader
	bucket     string
}

func NewS3Client(conf aws.Config, bucket string) (*S3Client, error) {
	sess, err := session.NewSession(&conf)
	if err != nil {
		return nil, err
	}

	return &S3Client{sess, s3.New(sess), s3manager.NewDownloader(sess), bucket}, nil
}

func (cl *S3Client) FileContents(key string) ([]byte, error) {
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := cl.downloader.Download(buf,
		&s3.GetObjectInput{
			Key:    aws.String(key),
			Bucket: aws.String(cl.bucket),
		})
	if err != nil {
		return nil, fmt.Errorf("unable to download item %q, %v", key, err)
	}
	return buf.Bytes(), err
}

func (cl *S3Client) List(path string) ([]string, error) {
	resp, err := cl.s3.ListObjects(
		&s3.ListObjectsInput{
			Bucket:    aws.String(cl.bucket),
			Prefix:    aws.String(path),
			Delimiter: aws.String("/"),
		})
	if err != nil {
		return nil, fmt.Errorf("error listing bucket: %v", err)
	}

	var keys []string
	for _, object := range resp.Contents {
		keys = append(keys, *object.Key)
	}
	return keys, nil
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

func (s *S3Storage) runCmd(run []string) (ExecResult, error) {
	var err error

	exec, err := RunCommand(s.ctx, s.host, run)
	cmdLine := strings.Join(run, " ")

	if err != nil {
		tracelog.ErrorLogger.Printf("Command failed '%s' failed: %v", cmdLine, exec.String())
		return exec, err
	}

	if exec.ExitCode != 0 {
		tracelog.ErrorLogger.Printf("Command failed '%s' failed: %v", cmdLine, exec.String())
		err = fmt.Errorf("command '%s' exit code: %d", cmdLine, exec.ExitCode)
	}

	return exec, err
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
		conf := aws.Config{
			Credentials:      credentials.NewStaticCredentials(s.access, s.secret, ""),
			Endpoint:         aws.String(fmt.Sprintf("http://%s:%d", host, port)),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String("test_region"),
		}

		client, err := NewS3Client(conf, s.bucket)
		if err != nil {
			return nil, err
		}
		s.client = client
	}

	return s.client, nil
}

func (s *S3Storage) InitMinio() error {
	var err error
	var response ExecResult
	for i := 0; i < 100; i++ {
		command := []string{"mc", "--debug", "config", "host", "add", "local", "http://localhost:9000", s.access, s.secret}
		response, err = RunCommand(s.ctx, s.host, command)
		command = []string{"mc", "mb", fmt.Sprintf("local/%s", s.bucket)}
		response, _ = RunCommand(s.ctx, s.host, command)
		if strings.Contains(response.Combined(), "created successfully") ||
			strings.Contains(response.Combined(), "already own it") {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !strings.Contains(response.Combined(), "created successfully") {
		return fmt.Errorf("s3 is not available %s: %s", err, response.Combined())
	}

	return nil
}

func (s *S3Storage) Archives() ([]Archive, error) {
	cl, err := s.Client()
	if err != nil {
		return nil, err
	}

	// TODO: remove hardcoded path
	keys, err := cl.List("mongodb-backup/test_uuid/test_mongodb/oplog_005/")
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
	for _, arch := range archives {
		if (LessTS(arch.StartTS, ts) && LessTS(ts, arch.EndTS)) || arch.EndTS == ts {
			return true, nil
		}
	}

	return false, fmt.Errorf("archive with ts '%v' was not found", ts)
}
