package walg

import (
	"archive/tar"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pierrec/lz4"
	"io"
	"os"
	"path/filepath"
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
	EXCLUDE["pg_control"] = Empty{}

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
	Tb      TarBall
	Tbm     TarBallMaker
}

func (b *Bundle) GetTarBall() TarBall { return b.Tb }
func (b *Bundle) NewTarBall()         { b.Tb = b.Tbm.Make() }

type TarBall interface {
	SetUp()
	CloseTar()
	Finish()
	BaseDir() string
	Trim() string
	Nop() bool
	Number() int
	Size() int64
	SetSize(int64)
	Tw() *tar.Writer
}

type FileTarBall struct {
	baseDir string
	trim    string
	out     string
	number  int
	size    int64
	nop     bool
	w       io.WriteCloser
	tw      *tar.Writer
}

func (fb *FileTarBall) SetUp() {
	if fb.tw == nil {
		name := filepath.Join(fb.out, "part_"+fmt.Sprintf("%0.3d", fb.number)+".tar.lz4")
		f, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		fb.w = &Lz4CascadeClose{lz4.NewWriter(f), f}
		fb.tw = tar.NewWriter(fb.w)
	}
}

func (fb *FileTarBall) CloseTar() {
	err := fb.tw.Close()
	if err != nil {
		panic(err)
	}

	err = fb.w.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("Closed")
}

func (fb *FileTarBall) Finish() {
	fmt.Printf("Wrote %d compressed tar files to %s.\n", fb.number, fb.out)
}

func (fb *FileTarBall) BaseDir() string { return fb.baseDir }
func (fb *FileTarBall) Trim() string    { return fb.trim }
func (fb *FileTarBall) Nop() bool       { return fb.nop }
func (fb *FileTarBall) Number() int     { return fb.number }
func (fb *FileTarBall) Size() int64     { return fb.size }
func (fb *FileTarBall) SetSize(i int64) { fb.size += i }
func (fb *FileTarBall) Tw() *tar.Writer { return fb.tw }

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

func (s *S3TarBall) SetUp() {
	if s.tw == nil {
		name := "part_" + fmt.Sprintf("%0.3d", s.number) + ".tar.lz4"
		w := s.StartUpload(name)
		s.w = w
		s.tw = tar.NewWriter(w)
	}
}

func (s *S3TarBall) CloseTar() {
	err := s.tw.Close()
	if err != nil {
		panic(err)
	}

	err = s.w.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("Closed")
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

	tupl.wg.Wait()
	fmt.Printf("Uploaded %d compressed tar files.\n", s.number)
}

func (s *S3TarBall) BaseDir() string { return s.baseDir }
func (s *S3TarBall) Trim() string    { return s.trim }
func (s *S3TarBall) Nop() bool       { return s.nop }
func (s *S3TarBall) Number() int     { return s.number }
func (s *S3TarBall) Size() int64     { return s.size }
func (s *S3TarBall) SetSize(i int64) { s.size += i }
func (s *S3TarBall) Tw() *tar.Writer { return s.tw }

type NOPTarBall struct {
	baseDir string
	trim    string
	nop     bool
	number  int
	size    int64
	tw      *tar.Writer
}

func (n *NOPTarBall) SetUp()    { return }
func (n *NOPTarBall) CloseTar() { return }
func (n *NOPTarBall) Finish()   { fmt.Printf("NOP: %d files.\n", n.number) }

func (n *NOPTarBall) BaseDir() string { return n.baseDir }
func (n *NOPTarBall) Trim() string    { return n.trim }
func (n *NOPTarBall) Nop() bool       { return n.nop }
func (n *NOPTarBall) Number() int     { return n.number }
func (n *NOPTarBall) Size() int64     { return n.size }
func (n *NOPTarBall) SetSize(i int64) { n.size += i }
func (n *NOPTarBall) Tw() *tar.Writer { return n.tw }

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
