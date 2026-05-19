package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// JoinPath is like path.Join but it removes "/" from the beginning and adds it to the end:
// JoinPath("/a", "b", "c") -> "a/b/c/"
func JoinPath(elem ...string) string {
	var res []string
	for _, e := range elem {
		if e != "" {
			res = append(res, strings.Trim(e, "/"))
		}
	}
	return strings.Join(res, "/")
}
func AddDelimiterToPath(path string) string {
	if strings.HasSuffix(path, "/") || path == "" {
		return path
	}
	return path + "/"
}

func GetPathFromPrefix(prefix string) (bucket, server string, err error) {
	bucket, server, err = ParsePrefixAsURL(prefix)
	if err != nil {
		return "", "", err
	}

	// Allover the code this parameter is concatenated with '/'.
	// TODO: Get rid of numerous string literals concatenated with this
	server = strings.Trim(server, "/")

	return bucket, server, nil
}

func ParsePrefixAsURL(prefix string) (bucket, server string, err error) {
	storageURL, err := url.Parse(prefix)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to parse url '%s'", prefix)
	}
	if storageURL.Scheme == "" || storageURL.Host == "" {
		return "", "", errors.Errorf("missing url scheme=%q and/or host=%q", storageURL.Scheme, storageURL.Host)
	}

	return storageURL.Host, storageURL.Path, nil
}

const (
	tmpTagRandomBytes = 8 // 16 chars / 64 random bits
	tmpTagTimeLayout  = "20060102T150405Z"
)

// NewTimestampRandomTag returns a timestamp-prefixed random tag.
//
// Example:
//
//	.tmp.20260428T113012Z-9f2c6a4b
func NewTimestampRandomTag() (string, error) {
	b := make([]byte, tmpTagRandomBytes)
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	return ".tmp." + time.Now().UTC().Format(tmpTagTimeLayout) + "-" + hex.EncodeToString(b[:]), nil
}

var tmpSuffixRE = regexp.MustCompile(fmt.Sprintf(
	`\.tmp\.`+
		`\d{8}T\d{6}Z`+
		`-`+
		`[0-9a-f]{%d}`+
		`$`,
	tmpTagRandomBytes*2, // two hex chars per byte
))

// HasTimestampRandomTmpSuffix returns true iff path ends with:
//
//	.tmp.<UTC timestamp to seconds>-<8 lowercase hex chars>
//
// It checks only the path string suffix; it does not stat the file.
func HasTimestampRandomTmpSuffix(path string) bool {
	return tmpSuffixRE.MatchString(filepath.Base(path))
}
