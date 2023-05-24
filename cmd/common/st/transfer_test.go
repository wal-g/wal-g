package st

import (
	"bytes"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func Test_validateFlags(t *testing.T) {
	tests := []struct {
		name           string
		source, target string
		concurrency    int
		wantErr        bool
	}{
		{"source empty", "", "abc", 1, true},
		{"source all", "all", "abc", 1, true},
		{"target all", "abc", "all", 1, true},
		{"same storages", "abc", "abc", 1, true},
		{"concurrency < 1", "source", "target", 0, true},
		{"valid", "source", "target", 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transferSourceStorage = tt.source
			targetStorage = tt.target
			transferConcurrency = tt.concurrency
			if err := validateFlags(); (err != nil) != tt.wantErr {
				t.Errorf("validateFlags() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_transferFiles(t *testing.T) {
	transferMax = 80
	transferConcurrency = 5
	transferOverwrite = false
	transferAppearanceChecksInterval = 0
	transferAppearanceChecks = 3

	source := memory.NewFolder("source/", memory.NewStorage())
	target := memory.NewFolder("target/", memory.NewStorage())

	for i := 0; i < 100; i++ {
		_ = source.PutObject("a/"+strconv.Itoa(i), &bytes.Buffer{})
	}

	for i := 0; i < 10; i++ {
		_ = target.PutObject("a/"+strconv.Itoa(i), &bytes.Buffer{})
	}

	err := transferFiles(source, target, "a/")
	assert.NoError(t, err)

	found := 0
	for i := 0; i < 100; i++ {
		exists, err := target.Exists("a/" + strconv.Itoa(i))
		assert.NoError(t, err)
		if exists {
			found++
		}
	}
	assert.Equal(t, 90, found)
}

func Test_listFilesToMove(t *testing.T) {
	t.Run("list files from parent dir only", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1/a", &bytes.Buffer{})
		_ = source.PutObject("2/a", &bytes.Buffer{})

		files, err := listFilesToMove(source, target, "1/")
		assert.NoError(t, err)

		require.Len(t, files, 1)
		assert.Equal(t, "1/a", files[0].GetName())
	})

	t.Run("exclude already existing files", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		_ = target.PutObject("1", &bytes.Buffer{})

		transferOverwrite = false
		transferMax = 100

		files, err := listFilesToMove(source, target, "/")
		assert.NoError(t, err)

		require.Len(t, files, 1)
		assert.Equal(t, "2", files[0].GetName())
	})

	t.Run("include existing files when overwrite allowed", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		_ = target.PutObject("1", &bytes.Buffer{})

		transferOverwrite = true
		transferMax = 100

		files, err := listFilesToMove(source, target, "/")
		assert.NoError(t, err)

		require.Len(t, files, 2)
	})

	t.Run("limit number of files", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		transferOverwrite = false
		transferMax = 1

		files, err := listFilesToMove(source, target, "/")
		assert.NoError(t, err)

		require.Len(t, files, 1)
	})

	t.Run("list all files when limit is negative", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		transferOverwrite = false
		transferMax = -1

		files, err := listFilesToMove(source, target, "/")
		assert.NoError(t, err)

		require.Len(t, files, 2)
	})
}

func Test_moveFileStep(t *testing.T) {
	transferOverwrite = false
	transferAppearanceChecksInterval = 0
	transferAppearanceChecks = 3

	t.Run("copy file on first step", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("abc"))

		state := transferFileState{
			path:            "1",
			copied:          false,
			prevCheck:       time.Time{},
			performedChecks: 0,
		}
		newState, err := moveFileStep(source, target, state)
		assert.NoError(t, err)
		assert.NotNil(t, newState)
		want := &transferFileState{
			path:            "1",
			copied:          true,
			prevCheck:       time.Time{},
			performedChecks: 0,
		}
		assert.Equal(t, want, newState)

		file, err := target.ReadObject("1")
		assert.NoError(t, err)
		if file != nil {
			content, err := io.ReadAll(file)
			assert.NoError(t, err)
			assert.Equal(t, "abc", string(content))
		}

		_, err = source.ReadObject("1")
		assert.NoError(t, err)
	})

	t.Run("check appearance on second step", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("abc"))

		state := transferFileState{
			path:            "1",
			copied:          true,
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newState, err := moveFileStep(source, target, state)
		assert.NoError(t, err)
		assert.NotNil(t, newState)
		assert.Equal(t, "1", newState.path)
		assert.Equal(t, true, newState.copied)
		assert.NotEqual(t, time.Time{}, newState.prevCheck)
		assert.Equal(t, uint(1), newState.performedChecks)

		_, err = source.ReadObject("1")
		assert.NoError(t, err)
	})

	t.Run("delete file if it has appeared", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("abc"))
		_ = target.PutObject("1", bytes.NewBufferString("abc"))

		state := transferFileState{
			path:            "1",
			copied:          true,
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newState, err := moveFileStep(source, target, state)
		assert.NoError(t, err)
		assert.Nil(t, newState)

		exists, err := source.Exists("1")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	transferAppearanceChecks = 0

	t.Run("delete file if checking is turned off", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("abc"))
		_ = target.PutObject("1", bytes.NewBufferString("abc"))

		state := transferFileState{
			path:            "1",
			copied:          true,
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		newState, err := moveFileStep(source, target, state)
		assert.NoError(t, err)
		assert.Nil(t, newState)

		exists, err := source.Exists("1")
		assert.NoError(t, err)
		assert.False(t, exists)

		_, err = target.ReadObject("1")
		assert.NoError(t, err)
	})

	transferAppearanceChecks = 3

	t.Run("produce error when checks number exceeded", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("abc"))

		state := transferFileState{
			path:            "1",
			copied:          true,
			prevCheck:       time.Time{},
			performedChecks: 0,
		}

		for i := 0; i < 2; i++ {
			newState, err := moveFileStep(source, target, state)
			assert.NoError(t, err)
			require.NotNil(t, newState)
			assert.Equal(t, uint(i+1), newState.performedChecks)
			state = *newState
		}
		_, err := moveFileStep(source, target, state)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "couldn't wait for the file to appear")

		_, err = source.ReadObject("1")
		assert.NoError(t, err)
	})

	t.Run("handle error with file copying", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		state := transferFileState{
			path:   "1",
			copied: false,
		}
		_, err := moveFileStep(source, target, state)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "can't read file")
	})
}

func Test_copyToTarget(t *testing.T) {
	t.Run("write new file", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("source"))

		err := copyToTarget(source, target, transferFileState{path: "1"})
		assert.NoError(t, err)

		file, err := target.ReadObject("1")
		assert.NoError(t, err)
		content, _ := io.ReadAll(file)
		assert.Equal(t, "source", string(content))
	})

	t.Run("overwrite existing file", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		_ = source.PutObject("1", bytes.NewBufferString("source"))
		_ = target.PutObject("1", bytes.NewBufferString("target"))

		err := copyToTarget(source, target, transferFileState{path: "1"})
		assert.NoError(t, err)

		file, err := target.ReadObject("1")
		assert.NoError(t, err)
		content, _ := io.ReadAll(file)
		assert.Equal(t, "source", string(content))
	})

	t.Run("handle read err", func(t *testing.T) {
		source := memory.NewFolder("source/", memory.NewStorage())
		target := memory.NewFolder("target/", memory.NewStorage())

		err := copyToTarget(source, target, transferFileState{path: "1"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can't read file")
	})
}

func Test_checkForAppearance(t *testing.T) {
	t.Run("wait until next check time", func(t *testing.T) {
		target := memory.NewFolder("target/", memory.NewStorage())
		_ = target.PutObject("1", &bytes.Buffer{})

		checkTime := time.Now()

		transferAppearanceChecksInterval = 100 * time.Millisecond
		state := transferFileState{
			path:      "1",
			prevCheck: checkTime.Add(-50 * time.Millisecond),
		}
		appeared, err := checkForAppearance(target, state)
		assert.GreaterOrEqual(t, time.Now(), checkTime.Add(50*time.Millisecond))
		assert.NoError(t, err)
		assert.True(t, appeared)
	})

	t.Run("dont wait if time has come", func(t *testing.T) {
		target := memory.NewFolder("target/", memory.NewStorage())
		_ = target.PutObject("1", &bytes.Buffer{})

		transferAppearanceChecksInterval = time.Hour
		state := transferFileState{
			path:      "1",
			prevCheck: time.Now().Add(-time.Hour),
		}
		appeared, err := checkForAppearance(target, state)
		assert.NoError(t, err)
		assert.True(t, appeared)
	})
}
