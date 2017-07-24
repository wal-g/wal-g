package walg

import (
	"archive/tar"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pierrec/lz4"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

/**
 *  Checks that the following environment variables are set:
 *  WALE_S3_PREFIX
 *  AWS_REGION
 *  AWS_ACCESS_KEY_ID
 *  AWS_SECRET_ACCESS_KEY
 *  AWS_SECURITY_TOKEN
 */
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

/**
 *  Configure uploader and connect to S3. Requires these environment variables:
 *  WALE_S3_PREFIX
 *  AWS_REGION
 *  AWS_ACCESS_KEY_ID
 *  AWS_SECRET_ACCESS_KEY
 *  AWS_SECURITY_TOKEN
 *  Able to configure the upload part size in the S3 uploader.
 *  ISSUES: 64MB will get an out of memory error
 */
func Configure() (*TarUploader, *Prefix) {
	chk := make(map[string]string)

	chk["WALE_S3_PREFIX"] = os.Getenv("WALE_S3_PREFIX")
	chk["AWS_REGION"] = os.Getenv("AWS_REGION")
	chk["AWS_ACCESS_KEY_ID"] = os.Getenv("AWS_ACCESS_KEY_ID")
	chk["AWS_SECRET_ACCESS_KEY"] = os.Getenv("AWS_SECRET_ACCESS_KEY")
	chk["AWS_SECURITY_TOKEN"] = os.Getenv("AWS_SECURITY_TOKEN")

	err := checkVar(chk)
	if serr, ok := err.(*UnsetEnvVarError); ok {
		fmt.Println(serr.Error())
		os.Exit(1)
	} else if err != nil {
		panic(err)
	}

	u, err := url.Parse(chk["WALE_S3_PREFIX"])
	if err != nil {
		panic(err)
	}

	bucket := u.Host
	server := u.Path[1:]
	region := chk["AWS_REGION"]

	pre := &Prefix{
		Creds:  credentials.NewStaticCredentials(chk["AWS_ACCESS_KEY_ID"], chk["AWS_SECRET_ACCESS_KEY"], chk["AWS_SECURITY_TOKEN"]),
		Bucket: aws.String(bucket),
		Server: aws.String(server),
	}

	config := &aws.Config{
		Region:      aws.String(region),
		Credentials: pre.Creds,
	}

	sess, err := session.NewSession(config)
	if err != nil {
		panic(err)
	}

	pre.Svc = s3.New(sess)

	/*** 	Create an uploader with S3 client and custom options	***/
	up := s3manager.NewUploaderWithClient(pre.Svc, func(u *s3manager.Uploader) {
		u.PartSize = 20 * 1024 * 1024 // 20MB per part
		u.Concurrency = 3
	})

	upload := &TarUploader{
		//Upl: s3manager.NewUploaderWithClient(pre.Svc),
		upl:    up,
		bucket: bucket,
		server: server,
		region: region,
		wg:     &sync.WaitGroup{},
	}

	return upload, pre
}

/**
 *  Creates a lz4 writer and runs upload in the background once
 *  a tar member is finished writing.
 */
func (s *S3TarBall) StartUpload(name string) io.WriteCloser {
	pr, pw := io.Pipe()
	tupl := s.tu

	path := tupl.server + "/basebackups_005/" + s.bkupName + "/tar_partitions/" + name
	input := &s3manager.UploadInput{
		Bucket: aws.String(tupl.bucket),
		Key:    aws.String(path),
		Body:   pr,
	}

	fmt.Println("PATH:", path)

	tupl.wg.Add(1)
	go func() {
		defer tupl.wg.Done()

		_, err := tupl.upl.Upload(input)
		if err != nil {
			panic(err)
		}

	}()
	return &Lz4CascadeClose{lz4.NewWriter(pw), pw}
}

/**
 *  Compress a WAL file using LZ4 and upload to S3.
 */
func (tu *TarUploader) UploadWal(path string) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	lz := &LzPipeWriter{
		chunk: f,
	}

	lz.Compress()
	p := tu.server + "/wal_005/" + filepath.Base(path) + ".lz4"
	input := &s3manager.UploadInput{
		Bucket: aws.String(tu.bucket),
		Key:    aws.String(p),
		Body:   lz.pr,
	}

	tu.wg.Add(1)

	go func() {
		defer tu.wg.Done()

		_, err := tu.upl.Upload(input)
		if err != nil {
			panic(err)
		}

	}()

	fmt.Println("WAL PATH:", p)

}

func (bundle *Bundle) UploadSentinel() {
	fileName := bundle.Sen.info.Name()
	info := bundle.Sen.info
	path := bundle.Sen.path

	bundle.NewTarBall()
	tarBall := bundle.Tb
	tarBall.SetUp("pg_control.tar.lz4")
	tarWriter := tarBall.Tw()

	fmt.Println("------------------------------------------", fileName)
	hdr, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		panic(err)
	}

	hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
	fmt.Println("NAME:", hdr.Name)

	err = tarWriter.WriteHeader(hdr)
	if err != nil {
		panic(err)
	}

	if info.Mode().IsRegular() {
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}

		lim := &io.LimitedReader{
			R: f,
			N: int64(hdr.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			panic(err)
		}

		tarBall.SetSize(hdr.Size)
		f.Close()
	}
	tarBall.CloseTar()
}

/**
 *  Creates the `backup_label` and `tablespace_map` files and uploads
 *  it to S3.
 */
func (bundle *Bundle) UploadLabelFiles(lb, sc string) {
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
		panic(err)
	}
	_, err = io.Copy(tarWriter, strings.NewReader(lb))
	if err != nil {
		panic(err)
	}

	shdr := &tar.Header{
		Name:     "tablespace_map",
		Mode:     int64(0600),
		Size:     int64(len(sc)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(shdr)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(tarWriter, strings.NewReader(sc))
	if err != nil {
		panic(err)
	}

	tarBall.CloseTar()
}
