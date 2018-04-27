package walg

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/aws/aws-sdk-go/service/s3"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"encoding/json"
)

// BackupTime is used to sort backups by
// latest modified time.
type BackupTime struct {
	Name        string
	Time        time.Time
	WalFileName string
}

// TimeSlice represents a backup and its
// last modified time.
type TimeSlice []BackupTime

func (p TimeSlice) Len() int {
	return len(p)
}

func (p TimeSlice) Less(i, j int) bool {
	return p[i].Time.After(p[j].Time)
}

func (p TimeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
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

type md5Reader struct {
	internal io.Reader
	md5      hash.Hash
}

func newMd5Reader(reader io.Reader) *md5Reader {
	return &md5Reader{internal: reader, md5: md5.New()}
}

func (r *md5Reader) Read(p []byte) (n int, err error) {
	n, err = r.internal.Read(p)
	if err != nil {
		return
	}
	_, err = r.md5.Write(p[:n])
	return
}

func (r *md5Reader) Sum() string {
	bytes := r.md5.Sum(nil)
	return hex.EncodeToString(bytes)
}
