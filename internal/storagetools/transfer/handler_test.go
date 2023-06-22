package transfer

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/memory/mock"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestTransferHandler_Handle_Backup(t *testing.T) {
	defaultHandler := func() *Handler {
		lister := NewAllBackupsFileLister(false, 1000, 4)
		return &Handler{
			source:     memory.NewFolder("source/", memory.NewStorage()),
			target:     memory.NewFolder("target/", memory.NewStorage()),
			fileLister: lister,
			cfg: &HandlerConfig{
				FailOnFirstErr:           false,
				Concurrency:              7,
				AppearanceChecks:         100,
				AppearanceChecksInterval: time.Millisecond,
			},
			fileStatuses:    new(sync.Map),
			jobRequirements: map[jobKey][]jobRequirement{},
		}
	}

	genBackupFiles := func(num, filesNum int) []string {
		backupPrefix := fmt.Sprintf("basebackups_005/base_00%d", num)
		files := []string{
			backupPrefix + "_backup_stop_sentinel.json",
		}
		for i := 1; i <= filesNum-1; i++ {
			files = append(files, fmt.Sprintf("%s/tar_partitions/part_%d.tar", backupPrefix, i))
		}
		return files
	}

	t.Run("move all backups", func(t *testing.T) {
		h := defaultHandler()

		for i := 1; i <= 4; i++ {
			for _, f := range genBackupFiles(i, i*100) {
				_ = h.source.PutObject(f, bytes.NewBufferString("abc"))
			}
		}

		err := h.Handle()
		require.NoError(t, err)

		for i := 1; i <= 4; i++ {
			for _, f := range genBackupFiles(i, i*100) {
				exists, err := h.source.Exists(f)
				require.NoError(t, err)
				require.False(t, exists)

				exists, err = h.target.Exists(f)
				require.NoError(t, err)
				require.True(t, exists)
			}
		}
	})

	t.Run("operate backup files in correct order", func(t *testing.T) {
		h := defaultHandler()
		sourceMock := mock.NewFolder(memory.NewFolder("source/", memory.NewStorage()))
		h.source = sourceMock
		targetMock := mock.NewFolder(memory.NewFolder("target/", memory.NewStorage()))
		h.target = targetMock

		for _, f := range genBackupFiles(1, 100) {
			_ = h.source.PutObject(f, bytes.NewBufferString("abc"))
		}

		var (
			dataFilesCopied = int32(0)
			sentinelDeleted = false
		)

		targetMock.PutObjectMock = func(name string, content io.Reader) error {
			if strings.HasSuffix(name, "_backup_stop_sentinel.json") {
				if atomic.LoadInt32(&dataFilesCopied) < 99 {
					t.Fatalf("sentinel file must be copied to target storage only after all other files")
				}
				return targetMock.MemFolder.PutObject(name, content)
			}
			go func() {
				time.Sleep(time.Millisecond)
				atomic.AddInt32(&dataFilesCopied, 1)
				_ = targetMock.MemFolder.PutObject(name, content)
			}()
			return nil
		}
		sourceMock.DeleteObjectsMock = func(objectRelativePaths []string) error {
			if strings.HasSuffix(objectRelativePaths[0], "_backup_stop_sentinel.json") {
				sentinelDeleted = true
			} else if !sentinelDeleted {
				t.Fatalf("sentinel file must be deleted from source storage before all other files")
			}
			return sourceMock.MemFolder.DeleteObjects(objectRelativePaths)
		}

		err := h.Handle()
		require.NoError(t, err)

		for _, f := range genBackupFiles(1, 100) {
			exists, err := h.source.Exists(f)
			require.NoError(t, err)
			require.False(t, exists)

			exists, err = h.target.Exists(f)
			require.NoError(t, err)
			require.True(t, exists)
		}
	})
}

func TestTransferHandler_Handle(t *testing.T) {
	defaultHandler := func() *Handler {
		return &Handler{
			source:     memory.NewFolder("source/", memory.NewStorage()),
			target:     memory.NewFolder("target/", memory.NewStorage()),
			fileLister: NewRegularFileLister("/", false, 100500),
			cfg: &HandlerConfig{
				FailOnFirstErr:           false,
				Concurrency:              5,
				AppearanceChecks:         3,
				AppearanceChecksInterval: 0,
			},
			fileStatuses:    new(sync.Map),
			jobRequirements: map[jobKey][]jobRequirement{},
		}
	}

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
		h := defaultHandler()
		h.fileLister.(*RegularFileLister).MaxFiles = 80

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
		targetMock := mock.NewFolder(memory.NewFolder("target/", memory.NewStorage()))

		putCalls := 0
		putCallsMux := new(sync.Mutex)
		targetMock.PutObjectMock = func(name string, content io.Reader) error {
			putCallsMux.Lock()
			defer putCallsMux.Unlock()
			putCalls++
			if putCalls%5 == 0 {
				return fmt.Errorf("test")
			}
			return targetMock.MemFolder.PutObject(name, content)
		}

		h := defaultHandler()
		h.target = targetMock

		for i := 0; i < 100; i++ {
			_ = h.source.PutObject(strconv.Itoa(i), &bytes.Buffer{})
		}

		err := h.Handle()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "finished with 20 errors")

		assert.Equal(t, 80, countFiles(h.target, 100))
		assert.Equal(t, 20, countFiles(h.source, 100))
	})

	t.Run("fail after first error if it is configured so", func(t *testing.T) {
		sourceMock := mock.NewFolder(memory.NewFolder("source/", memory.NewStorage()))

		delCalls := 0
		dellCallsMux := new(sync.Mutex)
		sourceMock.DeleteObjectsMock = func(paths []string) error {
			dellCallsMux.Lock()
			defer dellCallsMux.Unlock()
			delCalls++
			if delCalls > 15 {
				return fmt.Errorf("test")
			}
			return sourceMock.MemFolder.DeleteObjects(paths)
		}

		h := defaultHandler()
		h.source = sourceMock
		h.cfg.FailOnFirstErr = true

		for i := 0; i < 100; i++ {
			_ = h.source.PutObject(strconv.Itoa(i), &bytes.Buffer{})
		}

		err := h.Handle()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete file")

		assert.Equal(t, 100, countFiles(h.target, 100))
		assert.Equal(t, 85, countFiles(h.source, 100))
	})
}

func TestTransferHandler_saveRequirements(t *testing.T) {
	h := &Handler{jobRequirements: map[jobKey][]jobRequirement{}}
	file := FileToMove{
		path:        "1",
		copyAfter:   []string{"2", "3"},
		deleteAfter: []string{"4", "5"},
	}

	copyJobKey := jobKey{
		filePath: "1",
		jobType:  jobTypeCopy,
	}
	deleteJobKey := jobKey{
		filePath: "1",
		jobType:  jobTypeDelete,
	}

	h.saveRequirements(file)

	assert.Equal(t,
		[]jobRequirement{
			{
				filePath:  "2",
				minStatus: transferStatusAppeared,
			},
			{
				filePath:  "3",
				minStatus: transferStatusAppeared,
			},
		},
		h.jobRequirements[copyJobKey],
	)
	assert.Equal(t,
		[]jobRequirement{
			{
				filePath:  "4",
				minStatus: transferStatusDeleted,
			},
			{
				filePath:  "5",
				minStatus: transferStatusDeleted,
			},
		},
		h.jobRequirements[deleteJobKey],
	)
}

func TestTransferHandler_checkRequirements(t *testing.T) {
	h := &Handler{
		jobRequirements: map[jobKey][]jobRequirement{
			jobKey{filePath: "1", jobType: jobTypeDelete}: {
				{
					filePath:  "2",
					minStatus: transferStatusCopied,
				},
			},
			jobKey{filePath: "2", jobType: jobTypeDelete}: {
				{
					filePath:  "3",
					minStatus: transferStatusAppeared,
				},
			},
			jobKey{filePath: "3", jobType: jobTypeDelete}: {
				{
					filePath:  "4",
					minStatus: transferStatusAppeared,
				},
			},
		},
		fileStatuses: new(sync.Map),
	}
	h.fileStatuses.Store("2", transferStatusAppeared)
	h.fileStatuses.Store("3", transferStatusCopied)
	h.fileStatuses.Store("4", transferStatusFailed)

	t.Run("true if requirements are satisfied", func(t *testing.T) {
		ok, err := h.checkRequirements(transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeDelete,
			},
		})
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("false is requirements are not satisfied", func(t *testing.T) {
		ok, err := h.checkRequirements(transferJob{
			key: jobKey{
				filePath: "2",
				jobType:  jobTypeDelete,
			},
		})
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("throw error when required file is failed", func(t *testing.T) {
		_, err := h.checkRequirements(transferJob{
			key: jobKey{
				filePath: "3",
				jobType:  jobTypeDelete,
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `delete operation requires other file "4" to be appeared, but it's failed`)
	})
}

func TestTransferHandler_copyFile(t *testing.T) {
	defaultHandler := func() *Handler {
		return &Handler{
			source:       memory.NewFolder("source/", memory.NewStorage()),
			target:       memory.NewFolder("target/", memory.NewStorage()),
			fileStatuses: new(sync.Map),
		}
	}

	t.Run("write new file", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", bytes.NewBufferString("source"))

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeCopy,
			},
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
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeCopy,
			},
		}

		_, err := h.copyFile(job)
		require.NoError(t, err)

		file, err := h.target.ReadObject("1")
		assert.NoError(t, err)
		content, _ := io.ReadAll(file)
		assert.Equal(t, "source", string(content))
	})

	t.Run("provide new wait job and update status", func(t *testing.T) {
		h := defaultHandler()

		_ = h.source.PutObject("1", bytes.NewBufferString("source"))

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeCopy,
			},
		}

		newJob, err := h.copyFile(job)
		require.NoError(t, err)

		wantJob := &transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeWait,
			},
		}
		assert.Equal(t, wantJob, newJob)

		status, ok := h.fileStatuses.Load("1")
		require.True(t, ok)
		assert.Equal(t, transferStatusCopied, status)
	})

	t.Run("handle read err", func(t *testing.T) {
		h := defaultHandler()

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeCopy,
			},
		}

		_, err := h.copyFile(job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read file")
	})
}

func TestTransferHandler_aitFile(t *testing.T) {
	defaultHandler := func() *Handler {
		return &Handler{
			source:       memory.NewFolder("source/", memory.NewStorage()),
			target:       memory.NewFolder("target/", memory.NewStorage()),
			fileStatuses: new(sync.Map),
			cfg: &HandlerConfig{
				AppearanceChecks:         3,
				AppearanceChecksInterval: 0,
			},
		}
	}

	t.Run("provide wait job again if file has not appeared", func(t *testing.T) {
		h := defaultHandler()

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeWait,
			},
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newJob, err := h.waitFile(job)
		assert.NoError(t, err)
		assert.NotNil(t, newJob)
		assert.Equal(t, "1", newJob.key.filePath)
		assert.Equal(t, jobTypeWait, newJob.key.jobType)
		assert.NotEqual(t, time.Time{}, newJob.prevCheck)
		assert.Equal(t, uint(1), newJob.performedChecks)
		_, ok := h.fileStatuses.Load("1")
		assert.False(t, ok)
	})

	t.Run("provide delete job if file has appeared", func(t *testing.T) {
		h := defaultHandler()

		_ = h.target.PutObject("1", &bytes.Buffer{})

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeWait,
			},
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newJob, err := h.waitFile(job)
		assert.NoError(t, err)
		assert.NotNil(t, newJob)
		assert.Equal(t, "1", newJob.key.filePath)
		assert.Equal(t, jobTypeDelete, newJob.key.jobType)
		status, ok := h.fileStatuses.Load("1")
		assert.True(t, ok)
		assert.Equal(t, transferStatusAppeared, status)
	})

	t.Run("provide delete job if checking is turned off", func(t *testing.T) {
		h := defaultHandler()
		h.cfg.AppearanceChecks = 0

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeWait,
			},
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newJob, err := h.waitFile(job)
		assert.NoError(t, err)
		assert.NotNil(t, newJob)
		assert.Equal(t, "1", newJob.key.filePath)
		assert.Equal(t, jobTypeDelete, newJob.key.jobType)
	})

	t.Run("throw error when checks number exceeded", func(t *testing.T) {
		h := defaultHandler()

		job := transferJob{
			key: jobKey{
				filePath: "1",
				jobType:  jobTypeWait,
			},
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		for i := 0; i < 2; i++ {
			newJob, err := h.waitFile(job)
			assert.NoError(t, err)
			require.NotNil(t, newJob)
			assert.Equal(t, uint(i+1), newJob.performedChecks)
			job = *newJob
		}
		_, err := h.waitFile(job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "couldn't wait for the file to appear")
	})
}

func TestTransferHandler_deleteFile(t *testing.T) {
	h := &Handler{
		source:       memory.NewFolder("source/", memory.NewStorage()),
		target:       memory.NewFolder("target/", memory.NewStorage()),
		fileStatuses: new(sync.Map),
	}

	_ = h.source.PutObject("1", &bytes.Buffer{})

	job := transferJob{
		key: jobKey{
			filePath: "1",
			jobType:  jobTypeDelete,
		},
	}

	err := h.deleteFile(job)
	require.NoError(t, err)

	exists, err := h.source.Exists("1")
	require.NoError(t, err)
	assert.False(t, exists)

	status, ok := h.fileStatuses.Load("1")
	assert.True(t, ok)
	assert.Equal(t, transferStatusDeleted, status)
}

func TestTransferHandler_checkForAppearance(t *testing.T) {
	t.Run("wait until next check time", func(t *testing.T) {
		h := &Handler{
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &HandlerConfig{
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
		h := &Handler{
			target: memory.NewFolder("target/", memory.NewStorage()),
			cfg: &HandlerConfig{
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
