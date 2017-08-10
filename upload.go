package walg

import (
	"archive/tar"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MAXRETRIES is the maximum number of retries for upload.
var MAXRETRIES = 7

// MAXBACKOFF is the maxmimum backoff time in seconds for upload.
var MAXBACKOFF = float64(32)

// Checks that the following environment variables are set:
// WALE_S3_PREFIX
// AWS_REGION
// AWS_ACCESS_KEY_ID
// AWS_SECRET_ACCESS_KEY
// AWS_SECURITY_TOKEN
func checkVar(n map[string]string) error {
	u := &UnsetEnvVarError{
		names: make([]string, 0, 5),
	}
	for i, val := range n {
		if val == "" {
			u.names = append(u.names, i)
		}
	}
	if len(u.names) != 0 {
		return u
	}

	return nil
}

// Configure connects to S3 and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
//
// Requires these environment variables to be set:
// WALE_S3_PREFIX
// AWS_REGION
// AWS_ACCESS_KEY_ID
// AWS_SECRET_ACCESS_KEY
// AWS_SECURITY_TOKEN
//
// Able to configure the upload part size in the S3 uploader.
func Configure() (*TarUploader, *Prefix, error) {
	chk := make(map[string]string)

	chk["WALE_S3_PREFIX"] = os.Getenv("WALE_S3_PREFIX")
	chk["AWS_REGION"] = os.Getenv("AWS_REGION")
	chk["AWS_ACCESS_KEY_ID"] = os.Getenv("AWS_ACCESS_KEY_ID")
	chk["AWS_SECRET_ACCESS_KEY"] = os.Getenv("AWS_SECRET_ACCESS_KEY")
	chk["AWS_SECURITY_TOKEN"] = os.Getenv("AWS_SECURITY_TOKEN")

	err := checkVar(chk)
	if err != nil {
		return nil, nil, err
	}

	u, err := url.Parse(chk["WALE_S3_PREFIX"])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Configure: failed to parse url '%s'", chk["WALE_S3_PREFIX"])
	}

	bucket := u.Host
	server := u.Path[1:]
	region := chk["AWS_REGION"]

	pre := &Prefix{
		Bucket: aws.String(bucket),
		Server: aws.String(server),
	}

	config := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(chk["AWS_ACCESS_KEY_ID"], chk["AWS_SECRET_ACCESS_KEY"], chk["AWS_SECURITY_TOKEN"]),
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Configure: failed to create new session")
	}

	pre.Svc = s3.New(sess)

	upload := NewTarUploader(pre.Svc, bucket, server, region, MAXRETRIES, MAXBACKOFF)

	var con int
	conc, ok := os.LookupEnv("WALG_CONCURRENCY")
	if ok {
		con, err = strconv.Atoi(conc)
	} else {
		con = 3
	}

	upload.Upl = CreateUploader(pre.Svc, 20*1024*1024, con) //default 3 concurrency streams at 20MB

	return upload, pre, err
}

// CreateUploader returns an uploader with customizable concurrency
// and partsize.
func CreateUploader(svc s3iface.S3API, partsize, concurrency int) s3manageriface.UploaderAPI {
	up := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
		u.PartSize = int64(partsize)
		u.Concurrency = concurrency
	})
	return up
}

// Helper function to upload to S3. If an error occurs during upload, retries will
// occur in exponentially incremental seconds.
func (tu *TarUploader) upload(input *s3manager.UploadInput, path string) (err error) {
	upl := tu.Upl
	et := NewExpTicker(tu.MaxRetries, tu.MaxWait)

	for {
		_, e := upl.Upload(input)
		if e == nil {
			tu.Success = true
			break
		}

		if e != nil {
			// If compression failure, will not retry.
			if re, ok := e.(Lz4Error); ok {
				return re
			}
			et.Update()

			if et.retries > et.MaxRetries {
				err = e
				break
			}

			if multierr, ok := e.(s3manager.MultiUploadFailure); ok {
				log.Printf("upload: failed to upload '%s' with UploadID '%s'. Restarting in %v seconds", path, multierr.UploadID(), et.wait)
			} else {
				log.Printf("upload: failed to upload '%s'. Restarting in %v seconds", path, et.wait)
			}

		}

		et.Sleep()
	}
	return errors.Wrap(err, "")
}

// StartUpload creates a lz4 writer and runs upload in the background once
// a compressed tar member is finished writing.
func (s *S3TarBall) StartUpload(name string) io.WriteCloser {
	pr, pw := io.Pipe()
	tupl := s.tu

	path := tupl.server + "/basebackups_005/" + s.bkupName + "/tar_partitions/" + name
	input := &s3manager.UploadInput{
		Bucket: aws.String(tupl.bucket),
		Key:    aws.String(path),
		Body:   pr,
	}

	fmt.Printf("Starting part %d ...\n", s.number)

	tupl.wg.Add(1)
	go func() {
		defer tupl.wg.Done()

		err := tupl.upload(input, path)
		if re, ok := err.(Lz4Error); ok {

			log.Printf("FATAL: could not upload '%s' due to compression error\n%+v\n", path, re)
		}
		if err != nil {
			log.Printf("upload: could not upload '%s' after %v retries\n", path, tupl.MaxRetries)
			log.Printf("FATAL%v\n", err)
		}

	}()
	return &Lz4CascadeClose{lz4.NewWriter(pw), pw}
}

// UploadWal compresses a WAL file using LZ4 and uploads to S3. Returns
// the first error encountered and an empty string upon failure.
func (tu *TarUploader) UploadWal(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.Wrapf(err, "UploadWal: failed to open file %s\n", path)
	}

	lz := &LzPipeWriter{
		Input: f,
	}

	lz.Compress()

	p := tu.server + "/wal_005/" + filepath.Base(path) + ".lz4"
	input := &s3manager.UploadInput{
		Bucket: aws.String(tu.bucket),
		Key:    aws.String(p),
		Body:   lz.Output,
	}

	tu.wg.Add(1)
	go func() {
		defer tu.wg.Done()
		err = tu.upload(input, path)

	}()

	tu.Finish()
	fmt.Println("WAL PATH:", p)
	return p, err
}

// HandleSentinel uploads the compressed tar file of `pg_control`. Will only be called
// after the rest of the backup is successfully uploaded to S3. Returns
// an error upon failure.
func (bundle *Bundle) HandleSentinel() error {
	fileName := bundle.Sen.Info.Name()
	info := bundle.Sen.Info
	path := bundle.Sen.path

	bundle.NewTarBall()
	tarBall := bundle.Tb
	tarBall.SetUp("pg_control.tar.lz4")
	tarWriter := tarBall.Tw()

	hdr, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to grab header info")
	}

	hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
	fmt.Println(hdr.Name)

	err = tarWriter.WriteHeader(hdr)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to write header")
	}

	if info.Mode().IsRegular() {
		f, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "HandleSentinel: failed to open file %s\n", path)
		}

		lim := &io.LimitedReader{
			R: f,
			N: int64(hdr.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "HandleSentinel: copy failed")
		}

		tarBall.SetSize(hdr.Size)
		f.Close()
	}

	err = tarBall.CloseTar()
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to close tarball")
	}

	return nil
}

// HandleLabelFiles creates the `backup_label` and `tablespace_map` files and uploads
// it to S3. Returns error upon failure.
func (bundle *Bundle) HandleLabelFiles(lb, sc string) error {
	bundle.NewTarBall()
	tarBall := bundle.Tb
	tarBall.SetUp()
	tarWriter := tarBall.Tw()

	lhdr := &tar.Header{
		Name:     "backup_label",
		Mode:     int64(0600),
		Size:     int64(len(lb)),
		Typeflag: tar.TypeReg,
	}

	err := tarWriter.WriteHeader(lhdr)
	if err != nil {
		return errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(lb))
	if err != nil {
		return errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(lhdr.Name)

	shdr := &tar.Header{
		Name:     "tablespace_map",
		Mode:     int64(0600),
		Size:     int64(len(sc)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(shdr)
	if err != nil {
		return errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(sc))
	if err != nil {
		return errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(shdr.Name)

	err = tarBall.CloseTar()
	if err != nil {
		return errors.Wrap(err, "HandleLabelFiles: failed to close tarball")
	}

	return nil
}
