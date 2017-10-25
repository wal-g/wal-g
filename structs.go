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
	"log"
	"os"
	"strings"
	"sync"
)

// EXCLUDE is a list of excluded members from the bundled backup.
var EXCLUDE = make(map[string]Empty)

func init() {
	EXCLUDE["pg_log"] = Empty{}
	EXCLUDE["pg_xlog"] = Empty{}
	EXCLUDE["pg_wal"] = Empty{}

	EXCLUDE["pgsql_tmp"] = Empty{}
	EXCLUDE["postgresql.auto.conf.tmp"] = Empty{}
	EXCLUDE["postmaster.pid"] = Empty{}
	EXCLUDE["postmaster.opts"] = Empty{}
	EXCLUDE["recovery.conf"] = Empty{}

	// DIRECTORIES
	EXCLUDE["pg_dynshmem"] = Empty{}
	EXCLUDE["pg_notify"] = Empty{}
	EXCLUDE["pg_replslot"] = Empty{}
	EXCLUDE["pg_serial"] = Empty{}
	EXCLUDE["pg_stat_tmp"] = Empty{}
	EXCLUDE["pg_snapshots"] = Empty{}
	EXCLUDE["pg_subtrans"] = Empty{}
}

// Empty is used for channel signaling.
type Empty struct{}

// TarBundle represents one completed directory.
type TarBundle interface {
	NewTarBall()
	GetTarBall() TarBall
}

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall will be at least
// MinSize bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	MinSize int64
	Sen     *Sentinel
	Tb      TarBall
	Tbm     TarBallMaker
	Crypter OpenPGPCrypter
}

func (b *Bundle) GetTarBall() TarBall { return b.Tb }
func (b *Bundle) NewTarBall()         { b.Tb = b.Tbm.Make() }

// Sentinel is used to signal completion of a walked
// directory.
type Sentinel struct {
	Info os.FileInfo
	path string
}

// A TarBall represents one tar file.
type TarBall interface {
	SetUp(crypter Crypter, args ...string)
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

// S3TarBall represents a tar file that is
// going to be uploaded to S3.
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

// SetUp creates a new tar writer and starts upload to S3.
// Upload will block until the tar file is finished writing.
// If a name for the file is not given, default name is of
// the form `part_....tar.lz4`.
func (s *S3TarBall) SetUp(crypter Crypter, names ...string) {
	if s.tw == nil {
		var name string
		if len(names) > 0 {
			name = names[0]
		} else {
			name = "part_" + fmt.Sprintf("%0.3d", s.number) + ".tar.lz4"
		}
		w := s.StartUpload(name, crypter)

		s.w = w
		s.tw = tar.NewWriter(w)

	}
}

// CloseTar closes the tar writer, flushing any unwritten data
// to the underlying writer before also closing the underlying writer.
func (s *S3TarBall) CloseTar() error {
	err := s.tw.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close tar writer")
	}

	err = s.w.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close underlying writer")
	}
	fmt.Printf("Finished writing part %d.\n", s.number)
	return nil
}

// Finish writes an empty .json file and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in S3.
// an alert is given with the corresponding error.
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
	tupl.Finish()

	//If other parts are successful in uploading, upload json file.
	if tupl.Success {
		tupl.wg.Add(1)
		go func() {
			defer tupl.wg.Done()

			e := tupl.upload(input, path)
			if e != nil {
				log.Printf("upload: could not upload '%s' after %v retries\n", path, tupl.MaxRetries)
				err = errors.Wrap(e, "S3TarBall Finish: json failed to upload")
			}

		}()

		tupl.Finish()
	}

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

// TarUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call CreateUploader()
// in 'upload.go'.
type TarUploader struct {
	Upl          s3manageriface.UploaderAPI
	MaxRetries   int
	MaxWait      float64
	StorageClass string
	Success      bool
	bucket       string
	server       string
	region       string
	wg           *sync.WaitGroup
}

// NewTarUploader creates a new tar uploader without the actual
// S3 uploader. CreateUploader() is used to configure byte size and
// concurrency streams for the uploader.
func NewTarUploader(svc s3iface.S3API, bucket, server, region string, r int, w float64) *TarUploader {
	return &TarUploader{
		MaxRetries:   r,
		MaxWait:      w,
		StorageClass: "STANDARD",
		bucket:       bucket,
		server:       server,
		region:       region,
		wg:           &sync.WaitGroup{},
	}
}

// Finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (tu *TarUploader) Finish() {
	tu.wg.Wait()
	if !tu.Success {
		log.Printf("WAL-G could not complete upload.\n")
	}
}
