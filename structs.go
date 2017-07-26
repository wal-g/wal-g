package walg

import (
	"archive/tar"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

type TarBall interface {
	SetUp(args ...string)
	CloseTar() error
	Finish()
	BaseDir() string
	Trim() string
	Nop() bool
	Number() int
	Size() int64
	SetSize(int64)
	Tw() *tar.Writer
}

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
 *  Closes tar writer flushing any unwritten data to underlying writer before also
 *  closing the underlying writer.
 */
func (s *S3TarBall) CloseTar() error {
	err := s.tw.Close()
	if err != nil {
		return err
	}

	err = s.w.Close()
	if err != nil {
		return err
	}
	fmt.Println("Closed")
	return nil
}

/**
 *  Once a backup is finished uploading, an empty .json file is written
 *  and uploaded with the backup name. Waits until all files have been
 *  uploaded.
 */
func (s *S3TarBall) Finish() {
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

		_, err := tupl.upl.Upload(input)
		if err != nil {
			panic(err)
		}

	}()

	tupl.Finish()
	fmt.Printf("Uploaded %d compressed tar files.\n", s.number)
}

func (s *S3TarBall) BaseDir() string { return s.baseDir }
func (s *S3TarBall) Trim() string    { return s.trim }
func (s *S3TarBall) Nop() bool       { return s.nop }
func (s *S3TarBall) Number() int     { return s.number }
func (s *S3TarBall) Size() int64     { return s.size }
func (s *S3TarBall) SetSize(i int64) { s.size += i }
func (s *S3TarBall) Tw() *tar.Writer { return s.tw }

type TarUploader struct {
	upl    *s3manager.Uploader
	bucket string
	server string
	region string
	wg     *sync.WaitGroup
}

func (tu *TarUploader) Finish() {
	tu.wg.Wait()
}
