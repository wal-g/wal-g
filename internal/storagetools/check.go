package storagetools

import (
	"bytes"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
)

func HandleCheckRead(folder storage.Folder, filenames []string) error {
	_, _, err := folder.ListFolder()
	if err != nil {
		return fmt.Errorf("failed to list the storage: %v", err)
	}
	missing := make([]string, 0)
	for _, name := range filenames {
		ok, err := folder.Exists(name)
		if err != nil || !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("files are missing: %s", strings.Join(missing, ", "))
	}
	tracelog.InfoLogger.Println("Read check OK")
	return nil
}

func randomName(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	r.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

func HandleCheckWrite(folder storage.Folder) error {
	var filename string
	for {
		filename = randomName(32)
		ok, err := folder.Exists(filename)
		if err != nil {
			return fmt.Errorf("failed to read from the storage: %v", err)
		}
		if !ok {
			break
		}
	}
	err := folder.PutObject(filename, bytes.NewBufferString("test"))
	if folder.DeleteObjects([]string{filename}) != nil {
		slog.Warn(fmt.Sprintf("failed to clean temp files, %s left in storage", filename))
	}
	if err != nil {
		return fmt.Errorf("failed to write to the storage: %v", err)
	}
	tracelog.InfoLogger.Println("Write check OK")
	return nil
}
