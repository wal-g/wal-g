package walg

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/jackc/pgx"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
)

// MAXRETRIES is the maximum number of retries for upload.
var MAXRETRIES = 7

// MAXBACKOFF is the maxmimum backoff time in seconds for upload.
var MAXBACKOFF = float64(32)

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
	} else {
		// all other regions are strings
		return *output.LocationConstraint, nil
	}
}

// Configure connects to S3 and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
//
// Requires these environment variables to be set:
// WALE_S3_PREFIX
//
// Able to configure the upload part size in the S3 uploader.
func Configure() (*TarUploader, *Prefix, error) {
	wale_s3_prefix := os.Getenv("WALE_S3_PREFIX")
	if wale_s3_prefix == "" {
		return nil, nil, &UnsetEnvVarError{names: []string{"WALE_S3_PREFIX"}}
	}

	u, err := url.Parse(wale_s3_prefix)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Configure: failed to parse url '%s'", wale_s3_prefix)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, nil, fmt.Errorf("Missing url scheme=%q and/or host=%q", u.Scheme, u.Host)
	}

	bucket := u.Host
	var server = ""
	if len(u.Path) > 0 {
		// TODO: Unchecked assertion: first char is '/'
		server = u.Path[1:]
	}

	if len(server) > 0 && server[len(server)-1] == '/' {
		// Allover the code this parameter is concatenated with '/'.
		// TODO: Get rid of numerous string literals concatenated with this
		server = server[:len(server)-1]
	}

	config := defaults.Get().Config

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

	pre := &Prefix{
		Bucket: aws.String(bucket),
		Server: aws.String(server),
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Configure: failed to create new session")
	}

	pre.Svc = s3.New(sess)

	upload := NewTarUploader(pre.Svc, bucket, server, region, MAXRETRIES, MAXBACKOFF)

	var con int
	conc, ok := os.LookupEnv("WALG_UPLOAD_CONCURRENCY")
	if ok {
		con, err = strconv.Atoi(conc)
	} else {
		con = 10
	}

	storageClass, ok := os.LookupEnv("WALG_S3_STORAGE_CLASS")
	if ok {
		upload.StorageClass = storageClass
	}

	upload.Upl = CreateUploader(pre.Svc, 20*1024*1024, con) //default 10 concurrency streams at 20MB

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
				log.Printf("upload: failed to upload '%s' with UploadID '%s'. Restarting in %0.2f seconds", path, multierr.UploadID(), et.wait)
			} else {
				log.Printf("upload: failed to upload '%s': %s. Restarting in %0.2f seconds", path, e.Error(), et.wait)
			}

		}

		et.Sleep()
	}
	return errors.Wrap(err, "")
}

// createUploadInput creates a s3manager.UploadInput for a TarUploader using
// the specified path and reader.
func (tu *TarUploader) createUploadInput(path string, reader io.Reader) *s3manager.UploadInput {
	return &s3manager.UploadInput{
		Bucket:       aws.String(tu.bucket),
		Key:          aws.String(path),
		Body:         reader,
		StorageClass: aws.String(tu.StorageClass),
	}
}

// StartUpload creates a lz4 writer and runs upload in the background once
// a compressed tar member is finished writing.
func (s *S3TarBall) StartUpload(name string, crypter Crypter) io.WriteCloser {
	pr, pw := io.Pipe()
	tupl := s.tu

	path := tupl.server + "/basebackups_005/" + s.bkupName + "/tar_partitions/" + name
	input := tupl.createUploadInput(path, pr)

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

	if crypter.IsUsed() {
		wc, err := crypter.Encrypt(pw)

		if err != nil {
			panic(err)
		}

		return &Lz4CascadeClose2{lz4.NewWriter(wc), wc, pw}
	}

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

	lz.Compress(&OpenPGPCrypter{})

	p := sanitizePath(tu.server + "/wal_005/" + filepath.Base(path) + ".lz4")
	input := tu.createUploadInput(p, lz.Output)

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
	tarBall.SetUp(&bundle.Crypter, "pg_control.tar.lz4")
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
// it to S3 by stopping the backup. Returns error upon failure.
func (bundle *Bundle) HandleLabelFiles(conn *pgx.Conn) (uint64, error) {
	var lb string
	var sc string
	var lsnStr string

	queryBuilder := PgQueryBuilder{}
	queryRunner := PgQueryRunner{queryBuilder: queryBuilder, connection: conn}
	lb, sc, lsnStr, err := queryRunner.StopBackup()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to stop backup")
	}

	lsn, err := ParseLsn(lsnStr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to parse finish LSN")
	}

	bundle.NewTarBall()
	tarBall := bundle.Tb
	tarBall.SetUp(&bundle.Crypter)
	tarWriter := tarBall.Tw()

	lhdr := &tar.Header{
		Name:     "backup_label",
		Mode:     int64(0600),
		Size:     int64(len(lb)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(lhdr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(lb))
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: copy failed")
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
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(sc))
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(shdr.Name)

	err = tarBall.CloseTar()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to close tarball")
	}

	return lsn, nil
}
