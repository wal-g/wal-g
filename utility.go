package walg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/wal-g/wal-g/tracelog"
	"io"
	"os"
	"path"
	"path/filepath"
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
	NoSuchKeyAWSErrorCode  = "NoSuchKey"
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

func sanitizePath(path string) string {
	return strings.TrimLeft(path, "/")
}

// TODO : unit tests
func partitionStrings(strings []string, blockSize int) [][]string {
	// I've unsuccessfully tried this with interface{} but there was too much of casting
	partition := make([][]string, 0)
	for i := 0; i < len(strings); i += blockSize {
		if i+blockSize > len(strings) {
			partition = append(partition, strings[i:])
		} else {
			partition = append(partition, strings[i:i+blockSize])
		}
	}
	return partition
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

// TODO : unit tests
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

// TODO : unit tests
func stripBackupName(path string) string {
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
// Strips the backup WAL file name.
func stripWalFileName(path string) string {
	name := stripBackupName(path)
	name = strings.SplitN(name, "_D_", 2)[0]

	if strings.HasPrefix(name, backupNamePrefix) {
		return name[len(backupNamePrefix):]
	}
	return ""
}
