package walg

import (
	"archive/tar"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
	"sync"
)

var EXCLUDE = make(map[string]Empty)

/**
 *  List of excluded members from the bundled backup.
 */
func init() {
	EXCLUDE["pg_log"] = Empty{}
	EXCLUDE["pg_xlog"] = Empty{}
	EXCLUDE["pg_wal"] = Empty{}

	EXCLUDE["pgsql_tmp"] = Empty{}
	EXCLUDE["postgresql.auto.conf.tmp"] = Empty{}
	EXCLUDE["postmaster.pid"] = Empty{}
	EXCLUDE["postmaster.opts"] = Empty{}
	EXCLUDE["recovery.conf"] = Empty{}

	/*** DIRECTORIES ***/
	EXCLUDE["pg_dynshmem"] = Empty{}
	EXCLUDE["pg_notify"] = Empty{}
	EXCLUDE["pg_replslot"] = Empty{}
	EXCLUDE["pg_serial"] = Empty{}
	EXCLUDE["pg_stat_tmp"] = Empty{}
	EXCLUDE["pg_snapshots"] = Empty{}
	EXCLUDE["pg_subtrans"] = Empty{}
}

type Empty struct{}

type TarBundle interface {
	NewTarBall()
	GetTarBall() TarBall
}

/*** CONCRETE TAR BUNDLE ***/
type Bundle struct {
	MinSize int64
	Sen     *Sentinel
	Tb      TarBall
	Tbm     TarBallMaker
}

func (b *Bundle) GetTarBall() TarBall { return b.Tb }
func (b *Bundle) NewTarBall()         { b.Tb = b.Tbm.Make() }

type Sentinel struct {
	Info os.FileInfo
	path string
}

/**
 *  Represents one tar file.
 */
type TarBall interface {
	SetUp(args ...string)
	CloseTar() error
	Finish() error
	BaseDir() string
	Trim() string
	Nop() bool
	Number() int
	Size() int64
	SetSize(int64)
	Tw() *tar.Writer
}

/**
 *  Represents tar file that is
 *  going to be uploaded to S3.
 */
type S3TarBall struct {
	baseDir  string
	trim     string
	bkupName string
	nop      bool
	number   int
	size     int64
	w        io.WriteCloser
	tw       *tar.Writer
	tu       *TarUploader
}

/**
 *  Creates a new tar writer and starts upload to S3.
 *  Upload will block until tar is finished writing. If a name
 *  for the file is not given, default name is of the form
 *  `part_....tar.lz4`.
 */
func (s *S3TarBall) SetUp(names ...string) {
	if s.tw == nil {
		var name string
		if len(names) > 0 {
			name = names[0]
		} else {
			name = "part_" + fmt.Sprintf("%0.3d", s.number) + ".tar.lz4"
		}
		w := s.StartUpload(name)
		s.w = w
		s.tw = tar.NewWriter(w)

	}
}

/**
 *  Closes tar writer flushing any unwritten data to underlying writer before
 *  closing underlying writer.
 */
func (s *S3TarBall) CloseTar() error {
	err := s.tw.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close tar writer")
	}

	err = s.w.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close underlying writer")
	}
	fmt.Println("Closed")
	return nil
}

/**
 *  Once a backup is finished uploading, an empty .json file is written
 *  and uploaded with the backup name. Waits until all files have been
 *  uploaded.
 */
func (s *S3TarBall) Finish() error {
	var err error
	tupl := s.tu
	body := "{}"
	name := s.bkupName + "_backup_stop_sentinel.json"
	path := tupl.server + "/basebackups_005/" + name
	input := &s3manager.UploadInput{
		Bucket: aws.String(tupl.bucket),
		Key:    aws.String(path),
		Body:   strings.NewReader(body),
	}
	tupl.wg.Add(1)
	go func() {
		defer tupl.wg.Done()

		e := tupl.upload(input, path)
		if e != nil {
			fmt.Printf("upload: could not upload '%s' after %v retries\n", path, tupl.MaxRetries)
			err = errors.Wrap(e, "S3TarBall Finish: json failed to upload")
		}

	}()

	tupl.Finish()
	if err == nil && tupl.Success {
		fmt.Printf("Uploaded %d compressed tar files.\n", s.number)
	}
	return err

}

func (s *S3TarBall) BaseDir() string { return s.baseDir }
func (s *S3TarBall) Trim() string    { return s.trim }
func (s *S3TarBall) Nop() bool       { return s.nop }
func (s *S3TarBall) Number() int     { return s.number }
func (s *S3TarBall) Size() int64     { return s.size }
func (s *S3TarBall) SetSize(i int64) { s.size += i }
func (s *S3TarBall) Tw() *tar.Writer { return s.tw }

/**
 *  Uploader associated with tarballs. Multiple tarballs can share
 *  one uploader. Must call CreateUploader() in 'upload.go'.
 */
type TarUploader struct {
	Upl        s3manageriface.UploaderAPI
	MaxRetries int
	MaxWait    float64
	Success    bool
	bucket     string
	server     string
	region     string
	wg         *sync.WaitGroup
}

/**
 *  Creates a new tar uploader with own waitgroup.
 */
func NewTarUploader(svc s3iface.S3API, bucket, server, region string, r int, w float64) *TarUploader {
	return &TarUploader{
		MaxRetries: r,
		MaxWait:    w,
		bucket:     bucket,
		server:     server,
		region:     region,
		wg:         &sync.WaitGroup{},
	}
}

func (tu *TarUploader) Finish() {
	tu.wg.Wait()
	if !tu.Success {
		fmt.Printf("WAL-G could not complete upload\n")
	}
}
