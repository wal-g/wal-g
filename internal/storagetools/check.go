package storagetools

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleCheckRead(ctx context.Context, folder storage.Folder, filenames []string) error {
	_, _, err := folder.ListFolder(ctx)
	if err != nil {
		return fmt.Errorf("failed to list the storage: %v", err)
	}
	missing := make([]string, 0)
	for _, name := range filenames {
		ok, err := folder.Exists(ctx, name)
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

func HandleCheckWrite(ctx context.Context, folder storage.Folder) error {
	var filename string
	for {
		filename = randomName(32)
		ok, err := folder.Exists(ctx, filename)
		if err != nil {
			return fmt.Errorf("failed to read from the storage: %v", err)
		}
		if !ok {
			break
		}
	}
	err := folder.PutObject(ctx, filename, bytes.NewBufferString("test"))
	if folder.DeleteObjects(ctx, []storage.Object{storage.NewLocalObject(filename, time.Time{}, 0)}) != nil {
		tracelog.WarningLogger.Printf("failed to clean temp files, %s left in storage", filename)
	}
	if err != nil {
		return fmt.Errorf("failed to write to the storage: %v", err)
	}
	tracelog.InfoLogger.Println("Write check OK")
	return nil
}
