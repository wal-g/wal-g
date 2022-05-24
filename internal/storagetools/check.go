package storagetools

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
)

func HandleCheckRead(folder storage.Folder, filenames []string) {
	_, _, err := folder.ListFolder()
	if err != nil {
		tracelog.ErrorLogger.Fatalf("failed to list the storage: %v", err)
	}
	missing := make([]string, 0)
	for _, name := range filenames {
		ok, err := folder.Exists(name)
		if err != nil || !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		tracelog.ErrorLogger.Fatalf("files are missing: %s", strings.Join(missing, ", "))
	}
	fmt.Printf("OK")
}

func randomName(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

func HandleCheckWrite(folder storage.Folder) {
	var filename string
	for {
		filename = randomName(32)
		ok, err := folder.Exists(filename)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("failed to read from the storage: %v", err)
		}
		if !ok {
			break
		}
	}
	err := folder.PutObject(filename, bytes.NewBufferString("test"))
	if folder.DeleteObjects([]string{filename}) != nil {
		tracelog.WarningLogger.Printf("failed to clean temp files, %s left in storage", filename)
	}
	if err != nil {
		tracelog.ErrorLogger.Fatalf("failed to write to the storage: %v", err)
	}
	fmt.Printf("OK")
}
