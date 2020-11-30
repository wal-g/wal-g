package utility

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

func LoggedClose(c io.Closer, errmsg string) {
	err := c.Close()
	if errmsg == "" {
		errmsg = "Problem with closing object"
	}
	if err != nil {
		tracelog.ErrorLogger.Printf("%s: %v", errmsg, err)
	}
}

const (
	VersionStr       = "005"
	BaseBackupPath   = "basebackups_" + VersionStr + "/"
	CatchupPath      = "catchup_" + VersionStr + "/"
	WalPath          = "wal_" + VersionStr + "/"
	BackupNamePrefix = "base_"
	BackupTimeFormat = "20060102T150405Z" // timestamps in that format should be lexicographically sorted

	// utility.SentinelSuffix is a suffix of backup finish sentinel file
	SentinelSuffix         = "_backup_stop_sentinel.json"
	CompressedBlockMaxSize = 20 << 20
	CopiedBlockMaxSize     = CompressedBlockMaxSize
	MetadataFileName       = "metadata.json"
	PathSeparator          = string(os.PathSeparator)
	Mebibyte               = 1024 * 1024
)

// not really the maximal value, but high enough.
var MaxTime time.Time

func init() {
	var err error
	MaxTime, err = time.Parse(BackupTimeFormat, "99991231T235959Z")
	if err != nil {
		panic(fmt.Sprintf("failed to parse MaxTime: %v", err))
	}
}

var MinTime = time.Unix(0, 0)

// Empty is used for channel signaling.
type Empty struct{}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func ToBytes(x interface{}) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, x)
	return buf.Bytes()
}

func AllZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func SanitizePath(path string) string {
	return strings.TrimLeft(path, PathSeparator)
}

func NormalizePath(path string) string {
	return strings.TrimRight(path, PathSeparator)
}

func IsInDirectory(path, directoryPath string) bool {
	relativePath, err := filepath.Rel(directoryPath, path)
	if err != nil {
		return false
	}
	return relativePath == "." || NormalizePath(NormalizePath(directoryPath)+PathSeparator+relativePath) == NormalizePath(path)
}

func PathsEqual(path1, path2 string) bool {
	return NormalizePath(path1) == NormalizePath(path2)
}

// utility.ResolveSymlink converts path to physical if it is symlink
func ResolveSymlink(path string) string {
	resolve, err := filepath.EvalSymlinks(path)
	if err != nil {
		// TODO: Consider descriptive panic here and other checks
		// Directory may be absent et c.
		return path
	}
	return resolve
}

func GetFileExtension(filePath string) string {
	ext := path.Ext(filePath)
	if ext != "" {
		ext = ext[1:]
	}
	return ext
}

func TrimFileExtension(filePath string) string {
	return strings.TrimSuffix(filePath, "."+GetFileExtension(filePath))
}

func GetSubdirectoryRelativePath(subdirectoryPath string, directoryPath string) string {
	return NormalizePath(SanitizePath(strings.TrimPrefix(subdirectoryPath, directoryPath)))
}

// BytesPool holds []byte.
type BytesPool struct {
	pool chan []byte
}

// NewBytesPool creates new BytesPool.
func NewBytesPool(max int) *BytesPool {
	return &BytesPool{
		pool: make(chan []byte, max),
	}
}

const CopyBytesPoolSize = 2

var copyBytesPool = NewBytesPool(CopyBytesPoolSize)

// Get borrows []byte from the pool.
func (p *BytesPool) Get() []byte {
	var buf []byte
	select {
	case buf = <-p.pool:
	default:
		buf = make([]byte, CopiedBlockMaxSize)
	}
	return buf
}

// Put returns []byte to the pool.
func (p *BytesPool) Put(b []byte) {
	select {
	case p.pool <- b:
	default:
	}
}

//FastCopy copies data from src to dst in blocks of CopiedBlockMaxSize bytes
func FastCopy(dst io.Writer, src io.Reader) (int64, error) {
	n := int64(0)
	buf := copyBytesPool.Get()
	defer copyBytesPool.Put(buf)

	for {
		m, readingErr := src.Read(buf)
		if readingErr != nil && readingErr != io.EOF {
			return n, readingErr
		}
		m, writingErr := dst.Write(buf[:m])
		n += int64(m)
		if writingErr != nil || readingErr == io.EOF {
			return n, writingErr
		}
	}
}

func StripBackupName(path string) string {
	all := strings.SplitAfter(path, "/")
	name := strings.Split(all[len(all)-1], "_backup")[0]
	return name
}

func StripPrefixName(path string) string {
	path = strings.Trim(path, "/")
	all := strings.SplitAfter(path, "/")
	name := all[len(all)-1]
	return name
}

// TODO : unit base_tests
var patternLSN = "[0-9A-F]{24}"
var regexpLSN = regexp.MustCompile(patternLSN)

// Strips the backup WAL file name.
func StripWalFileName(path string) string {
	foundLsn := regexpLSN.FindAllString(path, 2)
	if len(foundLsn) > 0 {
		return foundLsn[0]
	}
	return strings.Repeat("Z", 24)
}

type ForbiddenActionError struct {
	error
}

func NewForbiddenActionError(message string) ForbiddenActionError {
	return ForbiddenActionError{errors.New(message)}
}

func (err ForbiddenActionError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// This function is needed for being cross-platform
func CeilTimeUpToMicroseconds(timeToCeil time.Time) time.Time {
	if timeToCeil.Nanosecond()%1000 != 0 {
		timeToCeil = timeToCeil.Add(time.Microsecond)
		timeToCeil = timeToCeil.Add(-time.Duration(timeToCeil.Nanosecond() % 1000))
	}
	return timeToCeil
}

func TimeNowCrossPlatformUTC() time.Time {
	return CeilTimeUpToMicroseconds(time.Now().In(time.UTC))
}

func TimeNowCrossPlatformLocal() time.Time {
	return CeilTimeUpToMicroseconds(time.Now())
}

var patternTimeRFC3339 = "[0-9]{8}T[0-9]{6}Z"
var regexpTimeRFC3339 = regexp.MustCompile(patternTimeRFC3339)

func TryFetchTimeRFC3999(name string) (string, bool) {
	times := regexpTimeRFC3339.FindAllString(name, 1)
	if len(times) > 0 {
		return times[0], true
	}
	return "", false
}

func ConcatByteSlices(a []byte, b []byte) []byte {
	result := make([]byte, len(a)+len(b))
	copy(result, a)
	copy(result[len(a):], b)
	return result
}

func SelectMatchingFiles(fileMask string, filePathsToFilter map[string]bool) (map[string]bool, error) {
	if fileMask == "" {
		return filePathsToFilter, nil
	}
	fileMask = "/" + fileMask
	result := make(map[string]bool)
	for filePathToFilter := range filePathsToFilter {
		matches, err := filepath.Match(fileMask, filePathToFilter)
		if err != nil {
			return nil, err
		}
		if matches {
			result[filePathToFilter] = true
		}
	}
	return result, nil
}

// ResetTimer safety resets timer (drains channel if required)
func ResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// SignalHandler defines signal handler setup & shutdown representation
type SignalHandler struct {
	ctx    context.Context
	ch     chan os.Signal
	cancel func()
}

// NewSignalHandler constructs SignalHandler and sets up signal mask
func NewSignalHandler(ctx context.Context, cancel func(), signals []os.Signal) *SignalHandler {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, signals...)
	sh := SignalHandler{ctx: ctx, ch: ch, cancel: cancel}
	go func() {
		select {
		case s := <-sh.ch:
			tracelog.InfoLogger.Printf("Received %s signal. Shutting down", s.String())
			sh.cancel()
		case <-sh.ctx.Done():
		}
	}()
	return &sh
}

// Close removes signal mask and call cancel func
func (sh *SignalHandler) Close() error {
	tracelog.InfoLogger.Printf("Removing sigmask. Shutting down")
	signal.Stop(sh.ch)
	sh.cancel()
	return nil
}

func StartCommandWithStdoutStderr(cmd *exec.Cmd) (io.ReadCloser, *bytes.Buffer, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	err = cmd.Start()
	if err != nil {
		return nil, nil, err
	}
	return stdout, stderr, err
}

func StartCommandWithStdoutPipe(cmd *exec.Cmd) (io.ReadCloser, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	return stdout, err
}

func ParseUntilTs(untilTs string) (time.Time, error) {
	if untilTs != "" {
		dt, err := time.Parse(time.RFC3339, untilTs)
		if err != nil {
			return time.Time{}, err
		}
		return dt, nil
	}
	// far future
	return MaxTime, nil
}
