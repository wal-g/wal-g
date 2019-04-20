package internal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	VersionStr       = "005"
	BaseBackupPath   = "basebackups_" + VersionStr + "/"
	WalPath          = "wal_" + VersionStr + "/"
	backupNamePrefix = "base_"

	// SentinelSuffix is a suffix of backup finish sentinel file
	SentinelSuffix         = "_backup_stop_sentinel.json"
	CompressedBlockMaxSize = 20 << 20
	NotFoundAWSErrorCode   = "NotFound"
	MetadataFileName       = "metadata.json"
)

// Empty is used for channel signaling.
type Empty struct{}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func ToBytes(x interface{}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, x)
	return buf.Bytes()
}

func allZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func SanitizePath(path string) string {
	return strings.TrimLeft(path, "/")
}

// ResolveSymlink converts path to physical if it is symlink
func ResolveSymlink(path string) string {
	resolve, err := filepath.EvalSymlinks(path)
	if err != nil {
		// TODO: Consider descriptive panic here and other checks
		// Directory may be absent et c.
		return path
	}
	return resolve
}

func getMaxDownloadConcurrency(defaultValue int) int {
	return getMaxConcurrency("WALG_DOWNLOAD_CONCURRENCY", defaultValue)
}

func getMaxUploadConcurrency(defaultValue int) int {
	return getMaxConcurrency("WALG_UPLOAD_CONCURRENCY", defaultValue)
}

// This setting is intentially undocumented in README. Effectively, this configures how many prepared tar Files there
// may be in uploading state during backup-push.
func getMaxUploadQueue() int {
	return getMaxConcurrency("WALG_UPLOAD_QUEUE", 2)
}

// GetSentinelUserData tries to parse WALG_SENTINEL_USER_DATA env variable
func GetSentinelUserData() interface{} {
	dataStr, ok := os.LookupEnv("WALG_SENTINEL_USER_DATA")
	if !ok || len(dataStr) == 0 {
		return nil
	}
	var out interface{}
	err := json.Unmarshal([]byte(dataStr), &out)
	if err != nil {
		tracelog.WarningLogger.Println("Unable to parse WALG_SENTINEL_USER_DATA as JSON")
		return dataStr
	}
	return out
}

func getMaxUploadDiskConcurrency() int {
	return getMaxConcurrency("WALG_UPLOAD_DISK_CONCURRENCY", 1)
}

// TODO : unit tests
func getMaxConcurrency(key string, defaultValue int) int {
	var con int
	var err error
	conc, ok := os.LookupEnv(key)
	if ok {
		con, err = strconv.Atoi(conc)

		if err != nil {
			tracelog.ErrorLogger.Panic("Unknown concurrency number ", err)
		}
	} else {
		if defaultValue > 0 {
			con = defaultValue
		} else {
			con = 10
		}
	}
	return max(con, 1)
}

func GetFileExtension(filePath string) string {
	ext := path.Ext(filePath)
	if ext != "" {
		ext = ext[1:]
	}
	return ext
}

func GetFileRelativePath(fileAbsPath string, directoryPath string) string {
	return strings.TrimPrefix(fileAbsPath, directoryPath)
}

// TODO : unit tests
func FastCopy(dst io.Writer, src io.Reader) (int64, error) {
	n := int64(0)
	buf := make([]byte, CompressedBlockMaxSize)
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

// TODO : unit tests
func stripPrefixName(path string) string {
	path = strings.Trim(path, "/")
	all := strings.SplitAfter(path, "/")
	name := all[len(all)-1]
	return name
}

// TODO : unit tests
var patternLSN = "[0-9A-F]{24}"
var regexpLSN = regexp.MustCompile(patternLSN)

// Strips the backup WAL file name.
func stripWalFileName(path string) string {
	found_lsn := regexpLSN.FindAllString(path, 2)
	if len(found_lsn) > 0 {
		return found_lsn[0]
	}
	return strings.Repeat("Z", 24)
}

func LoggedClose(obj io.Closer) {
	if err := obj.Close(); err != nil {
		tracelog.ErrorLogger.Printf("Problem with closing object: %v", err)
	}
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
