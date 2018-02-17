package walg

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
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
	// I've unsuccesfully tried this with interface{} but there was too much of casting
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

func ResolveSymlink(path string) string {
	resolve, err := filepath.EvalSymlinks(path)
	if err != nil {
		// TODO: Consider descriptive panic here and other checks
		// Directory may be absent et c.
		return path
	}
	return resolve
}

func getMaxConcurrency(reasonableMaximum int) int {
	var con int
	var err error
	conc, ok := os.LookupEnv("WALG_DOWNLOAD_CONCURRENCY")
	if ok {
		con, err = strconv.Atoi(conc)

		if err != nil {
			log.Panic("Unknown concurrency number ", err)
		}
	} else {
		con = min(10, reasonableMaximum)
	}
	return max(con, 1)
}
