package walg

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"encoding/json"
	"strings"
	"regexp"
	"io"
)

const (
	VersionStr       = "005"
	BaseBackupsPath  = "/basebackups_" + VersionStr + "/"
	WalPath          = "/wal_" + VersionStr + "/"
	backupNamePrefix = "base_"

	// SentinelSuffix is a suffix of backup finish sentinel file
	SentinelSuffix         = "_backup_stop_sentinel.json"
	CompressedBlockMaxSize = 20 << 20
	NotFoundAWSErrorCode   = "NotFound"
)

// Empty is used for channel signaling.
type Empty struct{}

// NilWriter to /dev/null
type NilWriter struct{}

// Write to /dev/null
func (nw *NilWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

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

func contains(s *[]string, e string) bool {
	//AB: Go is sick
	if s == nil {
		return false
	}
	for _, a := range *s {
		if a == e {
			return true
		}
	}
	return false
}

func sanitizePath(path string) string {
	return strings.TrimLeft(path, "/")
}

func partition(a []string, b int) [][]string {
	c := make([][]string, 0)
	for i := 0; i < len(a); i += b {
		if i+b > len(a) {
			c = append(c, a[i:])
		} else {
			c = append(c, a[i:i+b])
		}
	}
	return c
}

func partitionObjects(a []*s3.ObjectIdentifier, b int) [][]*s3.ObjectIdentifier {
	// I've unsuccessfully tried this with interface{} but there was too much of casting
	c := make([][]*s3.ObjectIdentifier, 0)
	for i := 0; i < len(a); i += b {
		if i+b > len(a) {
			c = append(c, a[i:])
		} else {
			c = append(c, a[i:i+b])
		}
	}
	return c
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

func getMaxDownloadConcurrency(default_value int) int {
	return getMaxConcurrency("WALG_DOWNLOAD_CONCURRENCY", default_value)
}

func getMaxUploadConcurrency(default_value int) int {
	return getMaxConcurrency("WALG_UPLOAD_CONCURRENCY", default_value)
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
		log.Println("WARNING! Unable to parse WALG_SENTINEL_USER_DATA as JSON")
		return dataStr
	}
	return out
}

func getMaxUploadDiskConcurrency() int {
	return getMaxConcurrency("WALG_UPLOAD_DISK_CONCURRENCY", 1)
}

func getMaxConcurrency(key string, default_value int) int {
	var con int
	var err error
	conc, ok := os.LookupEnv(key)
	if ok {
		con, err = strconv.Atoi(conc)

		if err != nil {
			log.Panic("Unknown concurrency number ", err)
		}
	} else {
		if default_value > 0 {
			con = default_value
		} else {
			con = 10
		}
	}
	return max(con, 1)
}

// CheckType grabs the file extension from PATH.
func CheckType(path string) string {
	re := regexp.MustCompile(`\.([^\.]+)$`)
	f := re.FindString(path)
	if f != "" {
		return f[1:]
	}
	return ""
}

func readFrom(dst io.Writer, src io.Reader) (n int64, err error) {
	buf := make([]byte, CompressedBlockMaxSize)
	for {
		m, er := io.ReadFull(src, buf)
		n += int64(m)
		if er == nil || er == io.ErrUnexpectedEOF || er == io.EOF {
			if _, err = dst.Write(buf[:m]); err != nil {
				return
			}
			if er == nil {
				continue
			}
			return
		}
		return n, er
	}
}
