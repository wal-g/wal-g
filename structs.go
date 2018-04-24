package walg

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
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

// NilWriter to /dev/null
type NilWriter struct{}

// Write to /dev/null
func (nw *NilWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// TarBundle represents one completed directory.
type TarBundle interface {
	NewTarBall(inheritState TarBall)
	GetIncrementBaseLsn() *uint64
	GetIncrementBaseFiles() BackupFileList

	StartQueue()
	Deque() TarBall
	EnqueueBack(tb TarBall, parallelOpInProgress *bool)
	CheckSizeAndEnqueueBack(tb TarBall) error
	FinishQueue() error
}

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall will be at least
// MinSize bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	MinSize            int64
	Sen                *Sentinel
	Tb                 TarBall
	Tbm                TarBallMaker
	Crypter            OpenPGPCrypter
	Timeline           uint32
	Replica            bool
	IncrementFromLsn   *uint64
	IncrementFromFiles BackupFileList

	tarballQueue     chan (TarBall)
	uploadQueue      chan (TarBall)
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
	started          bool
}

func (b *Bundle) StartQueue() {
	if b.started {
		panic("Trying to start already started Queue")
	}
	b.parallelTarballs = getMaxUploadDiskConcurrency()
	b.maxUploadQueue = getMaxUploadQueue()
	b.tarballQueue = make(chan (TarBall), b.parallelTarballs)
	b.uploadQueue = make(chan (TarBall), b.parallelTarballs + b.maxUploadQueue)
	for i := 0; i < b.parallelTarballs; i++ {
		b.NewTarBall(nil)
		b.tarballQueue <- b.Tb
	}
	b.started = true
}

func (b *Bundle) Deque() TarBall {
	if !b.started {
		panic("Trying to deque from not started Queue")
	}
	return <-b.tarballQueue
}

func (b *Bundle) FinishQueue() error {
	if !b.started {
		panic("Trying to stop not started Queue")
	}
	b.started = false


	// At this point no new tarballs should be put into uploadQueue
	for len(b.uploadQueue) > 0 {
		otb := <-b.uploadQueue
		otb.AwaitUploads()
	}

	b.NewTarBall(nil)
	files := b.Tb.GetFiles()
	for len(b.tarballQueue) > 0 {
		tb := <-b.tarballQueue
		if tb.Tw() == nil {
			// This had written nothing
			continue
		}
		err := tb.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalker: failed to close tarball")
		}
		tb.AwaitUploads()

		for k, v := range tb.GetFiles() {
			files[k] = v
		}
	}
	b.Tb.SetFiles(files)
	return nil
}

func (b *Bundle) EnqueueBack(tb TarBall, parallelOpInProgress *bool) {
	if !*parallelOpInProgress {
		b.tarballQueue <- tb
	}
}

func (b *Bundle) CheckSizeAndEnqueueBack(tb TarBall) error {
	if tb.Size() > b.MinSize {
		b.mutex.Lock()
		defer b.mutex.Unlock()

		err := tb.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalker: failed to close tarball")
		}

		b.uploadQueue <- tb
		for len(b.uploadQueue) > b.maxUploadQueue {
			select {
				case otb := <-b.uploadQueue:
					otb.AwaitUploads()
				default:
			}
		}

		b.NewTarBall(tb)
		tb = b.Tb
	}
	b.tarballQueue <- tb
	return nil
}

// NewTarBall starts writing new tarball
func (b *Bundle) NewTarBall(inheritState TarBall) {
	ntb := b.Tbm.Make(inheritState != nil)
	files := make(map[string]BackupFileDescription)

	if inheritState != nil {
		// Map of incremental files are inherited from previous Tar
		// from the same bundle in case of sequential tarball creation
		for k, v := range inheritState.GetFiles() {
			files[k] = v
		}
	}

	ntb.SetFiles(files)

	b.Tb = ntb
}

// GetIncrementBaseLsn returns LSN of previous backup
func (b *Bundle) GetIncrementBaseLsn() *uint64 { return b.IncrementFromLsn }

// GetIncrementBaseFiles returns list of files from previous backup
func (b *Bundle) GetIncrementBaseFiles() BackupFileList { return b.IncrementFromFiles }

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
	Finish(sentinel *S3TarBallSentinelDto) error
	BaseDir() string
	Trim() string
	Nop() bool
	Number() int
	Size() int64
	AddSize(int64)
	Tw() *tar.Writer
	SetFiles(files BackupFileList)
	GetFiles() BackupFileList
	AwaitUploads()
}

// BackupFileList is a map of file properties in a backup
type BackupFileList map[string]BackupFileDescription

// S3TarBall represents a tar file that is
// going to be uploaded to S3.
type S3TarBall struct {
	baseDir          string
	trim             string
	bkupName         string
	nop              bool
	number           int
	size             int64
	w                io.WriteCloser
	tw               *tar.Writer
	tu               *TarUploader
	Lsn              *uint64
	IncrementFromLsn *uint64
	IncrementFrom    string
	Files            BackupFileList
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

// ErrSentinelNotUploaded happens when upload of json sentinel failed
var ErrSentinelNotUploaded = errors.New("Sentinel was not uploaded due to timeline change during backup")

// SetFiles of this backup
func (s *S3TarBall) SetFiles(files BackupFileList) {
	s.Files = files
}

// GetFiles of this backup
func (s *S3TarBall) GetFiles() BackupFileList {
	return s.Files
}

func (b *S3TarBall) AwaitUploads() {
	b.tu.wg.Wait()
}

// S3TarBallSentinelDto describes file structure of json sentinel
type S3TarBallSentinelDto struct {
	LSN               *uint64
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	Files BackupFileList

	PgVersion int
	FinishLSN *uint64
}

// BackupFileDescription contains properties of one backup file
type BackupFileDescription struct {
	IsIncremented bool // should never be both incremented and Skipped
	IsSkipped     bool
	MTime         time.Time
}

// IsIncremental checks that sentinel represents delta backup
func (dto *S3TarBallSentinelDto) IsIncremental() bool {
	// If we have increment base, we must have all the rest properties.
	// If we do not have base - anything else is a mistake
	if dto.IncrementFrom != nil {
		if dto.IncrementFromLSN == nil || dto.IncrementFullName == nil || dto.IncrementCount == nil {
			panic("Inconsistent S3TarBallSentinelDto")
		}
	} else if dto.IncrementFromLSN != nil && dto.IncrementFullName != nil && dto.IncrementCount != nil {
		panic("Inconsistent S3TarBallSentinelDto")
	}
	return dto.IncrementFrom != nil
}

// Finish writes an empty .json file and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in S3.
// an alert is given with the corresponding error.
func (s *S3TarBall) Finish(sentinel *S3TarBallSentinelDto) error {
	var err error
	name := s.bkupName + "_backup_stop_sentinel.json"
	tupl := s.tu

	tupl.Finish()

	//If other parts are successful in uploading, upload json file.
	if tupl.Success && sentinel != nil {
		dtoBody, err := json.Marshal(*sentinel)
		if err != nil {
			return err
		}
		path := tupl.server + "/basebackups_005/" + name
		input := &s3manager.UploadInput{
			Bucket:       aws.String(tupl.bucket),
			Key:          aws.String(path),
			Body:         bytes.NewReader(dtoBody),
			StorageClass: aws.String(tupl.StorageClass),
		}

		tupl.wg.Add(1)
		go func() {
			defer tupl.wg.Done()

			e := tupl.upload(input, path)
			if e != nil {
				log.Printf("upload: could not upload '%s'\n", path)
				log.Fatalf("S3TarBall Finish: json failed to upload")
			}
		}()

		tupl.Finish()
	} else {
		log.Printf("Uploaded %d compressed tar files.\n", s.number)
		log.Printf("Sentinel was not uploaded %v", name)
		return ErrSentinelNotUploaded
	}

	if err == nil && tupl.Success {
		fmt.Printf("Uploaded %d compressed tar files.\n", s.number)
	}
	return err
}

// BaseDir of a backup
func (s *S3TarBall) BaseDir() string { return s.baseDir }

// Trim suffix
func (s *S3TarBall) Trim() string { return s.trim }

// Nop is a dummy fonction for test purposes
func (s *S3TarBall) Nop() bool { return s.nop }

// Number of parts
func (s *S3TarBall) Number() int { return s.number }

// Size accumulated in this tarball
func (s *S3TarBall) Size() int64 { return s.size }

// AddSize to total Size
func (s *S3TarBall) AddSize(i int64) { s.size += i }

// Tw is tar writer
func (s *S3TarBall) Tw() *tar.Writer { return s.tw }

// TarUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call CreateUploader()
// in 'upload.go'.
type TarUploader struct {
	Upl          s3manageriface.UploaderAPI
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
func NewTarUploader(svc s3iface.S3API, bucket, server, region string) *TarUploader {
	return &TarUploader{
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

// Clone creates similar TarUploader with new WaitGroup
func (tu *TarUploader) Clone() *TarUploader {
	return &TarUploader{
		tu.Upl,
		tu.StorageClass,
		tu.Success,
		tu.bucket,
		tu.server,
		tu.region,
		&sync.WaitGroup{},
	}
}
