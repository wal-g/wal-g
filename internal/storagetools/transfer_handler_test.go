package storagetools

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/memory/mock"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestTransferHandler_Handle(t *testing.T) {
	countFiles := func(folder storage.Folder, max int) int {
		found := 0
		for i := 0; i < max; i++ {
			exists, err := folder.Exists(strconv.Itoa(i))
			assert.NoError(t, err)
			if exists {
				found++
			}
		}
		return found
	}

	t.Run("move all nonexistent files", func(t *testing.T) {
		h := TransferHandler{
			source: memory.NewFolder("source/", memory.NewStorage()),
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &TransferHandlerConfig{
				Prefix:                   "/",
				Overwrite:                false,
				FailOnFirstErr:           false,
				Concurrency:              5,
				MaxFiles:                 80,
				AppearanceChecks:         3,
				AppearanceChecksInterval: 0,
			},
		}

		for i := 0; i < 100; i++ {
			_ = h.source.PutObject(strconv.Itoa(i), &bytes.Buffer{})
		}

		for i := 0; i < 10; i++ {
			_ = h.target.PutObject(strconv.Itoa(i), &bytes.Buffer{})
		}

		err := h.Handle()
		assert.NoError(t, err)

		assert.Equal(t, 90, countFiles(h.target, 100))
		assert.Equal(t, 20, countFiles(h.source, 100))
	})

	t.Run("tolerate errors with some files", func(t *testing.T) {
		target := mock.NewFolder(memory.NewFolder("target/", memory.NewStorage()))

		putCalls := 0
		putCallsMux := new(sync.Mutex)
		target.PutObjectMock = func(name string, content io.Reader) error {
			putCallsMux.Lock()
			defer putCallsMux.Unlock()
			putCalls++
			if putCalls%5 == 0 {
				return fmt.Errorf("test")
			}
			return target.MemFolder.PutObject(name, content)
		}

		h := TransferHandler{
			source: memory.NewFolder("source/", memory.NewStorage()),
			target: target,
			cfg: &TransferHandlerConfig{
				Prefix:                   "/",
				Overwrite:                false,
				FailOnFirstErr:           false,
				Concurrency:              5,
				MaxFiles:                 100500,
				AppearanceChecks:         3,
				AppearanceChecksInterval: 0,
			},
		}

		for i := 0; i < 100; i++ {
			_ = h.source.PutObject(strconv.Itoa(i), &bytes.Buffer{})
		}

		err := h.Handle()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "finished with 20 errors")

		assert.Equal(t, 80, countFiles(h.target, 100))
		assert.Equal(t, 20, countFiles(h.source, 100))
	})

	t.Run("fail after first error if its configured so", func(t *testing.T) {
		source := mock.NewFolder(memory.NewFolder("source/", memory.NewStorage()))

		delCalls := 0
		dellCallsMux := new(sync.Mutex)
		source.DeleteObjectsMock = func(paths []string) error {
			dellCallsMux.Lock()
			defer dellCallsMux.Unlock()
			delCalls++
			if delCalls > 15 {
				return fmt.Errorf("test")
			}
			return source.MemFolder.DeleteObjects(paths)
		}

		h := TransferHandler{
			source: source,
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &TransferHandlerConfig{
				Prefix:                   "/",
				Overwrite:                false,
				FailOnFirstErr:           true,
				Concurrency:              5,
				MaxFiles:                 100500,
				AppearanceChecks:         3,
				AppearanceChecksInterval: 0,
			},
		}

		for i := 0; i < 100; i++ {
			_ = h.source.PutObject(strconv.Itoa(i), &bytes.Buffer{})
		}

		err := h.Handle()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "can't delete file")

		assert.Equal(t, 100, countFiles(h.target, 100))
		assert.Equal(t, 85, countFiles(h.source, 100))
	})
}

func TestTransferHandler_listFilesToMove(t *testing.T) {
	defaultHandler := func() *TransferHandler {
		return &TransferHandler{
			source: memory.NewFolder("source/", memory.NewStorage()),
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &TransferHandlerConfig{
				Prefix:    "/",
				Overwrite: false,
				MaxFiles:  100,
			},
		}
	}

	t.Run("list files from parent dir only", func(t *testing.T) {
		h := defaultHandler()
		h.cfg.Prefix = "1/"

		_ = h.source.PutObject("1/a", &bytes.Buffer{})
		_ = h.source.PutObject("2/a", &bytes.Buffer{})

		files, err := h.listFilesToMove()
		assert.NoError(t, err)

		require.Len(t, files, 1)
		assert.Equal(t, "1/a", files[0].GetName())
	})

	t.Run("exclude already existing files", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", &bytes.Buffer{})
		_ = h.source.PutObject("2", &bytes.Buffer{})

		_ = h.target.PutObject("1", &bytes.Buffer{})

		files, err := h.listFilesToMove()
		assert.NoError(t, err)

		require.Len(t, files, 1)
		assert.Equal(t, "2", files[0].GetName())
	})

	t.Run("include existing files when overwrite allowed", func(t *testing.T) {
		h := defaultHandler()
		h.cfg.Overwrite = true

		_ = h.source.PutObject("1", &bytes.Buffer{})
		_ = h.source.PutObject("2", &bytes.Buffer{})

		_ = h.target.PutObject("1", &bytes.Buffer{})

		files, err := h.listFilesToMove()
		assert.NoError(t, err)

		require.Len(t, files, 2)
	})

	t.Run("dont include nonexistent files even when overwrite allowed", func(t *testing.T) {
		h := defaultHandler()
		h.cfg.Overwrite = true

		_ = h.source.PutObject("2", &bytes.Buffer{})

		_ = h.target.PutObject("1", &bytes.Buffer{})

		files, err := h.listFilesToMove()
		assert.NoError(t, err)

		require.Len(t, files, 1)
		assert.Equal(t, "2", files[0].GetName())
	})

	t.Run("limit number of files", func(t *testing.T) {
		h := defaultHandler()
		h.cfg.MaxFiles = 1

		_ = h.source.PutObject("1", &bytes.Buffer{})
		_ = h.source.PutObject("2", &bytes.Buffer{})

		files, err := h.listFilesToMove()
		assert.NoError(t, err)

		require.Len(t, files, 1)
	})
}

func TestTransferHandler_copyFile(t *testing.T) {
	defaultHandler := func() *TransferHandler {
		return &TransferHandler{
			source: memory.NewFolder("source/", memory.NewStorage()),
			target: memory.NewFolder("target/", memory.NewStorage()),
		}
	}

	t.Run("write new file", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", bytes.NewBufferString("source"))

		job := transferJob{
			jobType:  transferJobTypeCopy,
			filePath: "1",
		}

		_, err := h.copyFile(job)
		require.NoError(t, err)

		file, err := h.target.ReadObject("1")
		assert.NoError(t, err)
		content, _ := io.ReadAll(file)
		assert.Equal(t, "source", string(content))
	})

	t.Run("overwrite existing file", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", bytes.NewBufferString("source"))
		_ = h.target.PutObject("1", bytes.NewBufferString("target"))

		job := transferJob{
			jobType:  transferJobTypeCopy,
			filePath: "1",
		}

		_, err := h.copyFile(job)
		require.NoError(t, err)

		file, err := h.target.ReadObject("1")
		assert.NoError(t, err)
		content, _ := io.ReadAll(file)
		assert.Equal(t, "source", string(content))
	})

	t.Run("provide new delete job", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", bytes.NewBufferString("source"))

		job := transferJob{
			jobType:  transferJobTypeCopy,
			filePath: "1",
		}

		newJob, err := h.copyFile(job)
		require.NoError(t, err)

		wantJob := &transferJob{
			jobType:  transferJobTypeDelete,
			filePath: "1",
		}
		assert.Equal(t, wantJob, newJob)
	})

	t.Run("handle read err", func(t *testing.T) {
		h := defaultHandler()

		job := transferJob{
			jobType:  transferJobTypeCopy,
			filePath: "1",
		}

		_, err := h.copyFile(job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "can't read file")
	})
}

func TestTransferHandler_deleteFile(t *testing.T) {
	defaultHandler := func() *TransferHandler {
		return &TransferHandler{
			source: memory.NewFolder("source/", memory.NewStorage()),
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &TransferHandlerConfig{
				Prefix:                   "/",
				Overwrite:                false,
				AppearanceChecks:         3,
				AppearanceChecksInterval: 0,
			},
		}
	}

	t.Run("check for appearance before deleting", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", &bytes.Buffer{})

		job := transferJob{
			jobType:         transferJobTypeDelete,
			filePath:        "1",
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newJob, err := h.deleteFile(job)
		assert.NoError(t, err)
		assert.NotNil(t, newJob)
		assert.Equal(t, "1", newJob.filePath)
		assert.Equal(t, transferJobTypeDelete, newJob.jobType)
		assert.NotEqual(t, time.Time{}, newJob.prevCheck)
		assert.Equal(t, uint(1), newJob.performedChecks)

		_, err = h.source.ReadObject("1")
		assert.NoError(t, err)
	})

	t.Run("delete file if it has appeared", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", &bytes.Buffer{})
		_ = h.target.PutObject("1", &bytes.Buffer{})

		job := transferJob{
			jobType:         transferJobTypeDelete,
			filePath:        "1",
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newJob, err := h.deleteFile(job)
		assert.NoError(t, err)
		assert.Nil(t, newJob)

		exists, err := h.source.Exists("1")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("delete file instantly if checking is turned off", func(t *testing.T) {
		h := defaultHandler()
		h.cfg.AppearanceChecks = 0

		_ = h.source.PutObject("1", &bytes.Buffer{})

		job := transferJob{
			jobType:         transferJobTypeDelete,
			filePath:        "1",
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newJob, err := h.deleteFile(job)
		assert.NoError(t, err)
		assert.Nil(t, newJob)

		exists, err := h.source.Exists("1")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("throw error when checks number exceeded", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", &bytes.Buffer{})

		job := transferJob{
			jobType:         transferJobTypeDelete,
			filePath:        "1",
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		for i := 0; i < 2; i++ {
			newJob, err := h.deleteFile(job)
			assert.NoError(t, err)
			require.NotNil(t, newJob)
			assert.Equal(t, uint(i+1), newJob.performedChecks)
			job = *newJob
		}
		_, err := h.deleteFile(job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "couldn't wait for the file to appear")

		_, err = h.source.ReadObject("1")
		assert.NoError(t, err)
	})
}

func TestTransferHandler_checkForAppearance(t *testing.T) {
	t.Run("wait until next check time", func(t *testing.T) {
		h := &TransferHandler{
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &TransferHandlerConfig{
				AppearanceChecksInterval: 100 * time.Millisecond,
			},
		}

		_ = h.target.PutObject("1", &bytes.Buffer{})

		thisCheckTime := time.Now()
		prevCheckTime := thisCheckTime.Add(-50 * time.Millisecond)

		appeared, err := h.checkForAppearance(prevCheckTime, "1")
		assert.GreaterOrEqual(t, time.Now(), thisCheckTime.Add(50*time.Millisecond))
		assert.NoError(t, err)
		assert.True(t, appeared)
	})

	t.Run("dont wait if time has come", func(t *testing.T) {
		h := &TransferHandler{
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &TransferHandlerConfig{
				AppearanceChecksInterval: time.Hour,
			},
		}

		_ = h.target.PutObject("1", &bytes.Buffer{})

		prevCheckTime := time.Now().Add(-time.Hour)

		appeared, err := h.checkForAppearance(prevCheckTime, "1")
		assert.NoError(t, err)
		assert.True(t, appeared)
	})
}
