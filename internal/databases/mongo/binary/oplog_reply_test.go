package binary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	archivepkg "github.com/wal-g/wal-g/internal/databases/mongo/archive"
	archivemocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestBuildOplog(t *testing.T) {
	ts := models.Timestamp{TS: 123, Inc: 7}

	assert.Equal(t, models.OplogArchBasePath+models.ArchiveTypeOplog+"_123.7", buildOplog(ts))
}

func TestResolveOplogReplaySequenceFallsBackToFullList(t *testing.T) {
	since := models.Timestamp{TS: 600, Inc: 1}
	until := models.Timestamp{TS: 630, Inc: 1}
	firstArch := mustArchive(t, models.Timestamp{TS: 500, Inc: 0}, models.Timestamp{TS: 610, Inc: 1})
	lastArch := mustArchive(t, firstArch.End, models.Timestamp{TS: 630, Inc: 1})
	downloader := archivemocks.NewDownloader(t)

	expectedSince := buildOplog(models.Timestamp{TS: since.TS - 300, Inc: 0})
	expectedUntil := buildOplog(models.Timestamp{TS: until.TS + 30, Inc: until.Inc})

	var actualSince *string
	var actualUntil *string
	downloader.On(
		"ListOplogArchivesSegment",
		testifymock.MatchedBy(func(value *string) bool {
			actualSince = value
			return value != nil && *value == expectedSince
		}),
		testifymock.MatchedBy(func(value *string) bool {
			actualUntil = value
			return value != nil && *value == expectedUntil
		}),
	).Return([]models.Archive{firstArch}, nil).Once()
	downloader.On("ListOplogArchives").Return([]models.Archive{firstArch, lastArch}, nil).Once()

	got, err := resolveOplogReplaySequence(downloader, since, until)
	require.NoError(t, err)
	assert.Equal(t, archivepkg.Sequence{firstArch, lastArch}, got)
	require.NotNil(t, actualSince)
	require.NotNil(t, actualUntil)
	assert.Equal(t, expectedSince, *actualSince)
	assert.Equal(t, expectedUntil, *actualUntil)
}

func mustArchive(t *testing.T, start, end models.Timestamp) models.Archive {
	t.Helper()

	arch, err := models.NewArchive(start, end, "lz4", models.ArchiveTypeOplog)
	require.NoError(t, err)
	return arch
}
