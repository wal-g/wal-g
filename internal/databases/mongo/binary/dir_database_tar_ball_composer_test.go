package binary

import (
	"archive/tar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

func TestShouldSplitWiredTigerFile(t *testing.T) {
	for _, test := range []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "Unix collection", path: "/data/collection-1.wt", expected: true},
		{name: "Unix index", path: "/data/index-1.wt", expected: true},
		{name: "Windows collection", path: `C:\data\collection-1.wt`, expected: true},
		{name: "Windows index", path: `\data\index-1.wt`, expected: true},
		{name: "Other WiredTiger file", path: `C:\data\WiredTiger.wt`, expected: false},
		{name: "Wrong extension", path: `C:\data\collection-1.wt.tmp`, expected: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := &internal.ComposeFileInfo{Header: &tar.Header{Name: test.path}}
			assert.Equal(t, test.expected, shouldSplitWiredTigerFile(info))
		})
	}
}
