package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestLongestOplogSequenceFromIncludesStartingOperation(t *testing.T) {
	first := followTestArchive(t, 90, 100)
	second := followTestArchive(t, 100, 110)
	third := followTestArchive(t, 110, 120)

	sequence, err := LongestOplogSequenceFrom(
		[]models.Archive{third, first, second}, models.Timestamp{TS: 100}, false)

	require.NoError(t, err)
	require.Equal(t, Sequence{first, second, third}, sequence)
}

func TestLongestOplogSequenceFromResumesAfterDurableTimestamp(t *testing.T) {
	first := followTestArchive(t, 90, 100)
	second := followTestArchive(t, 100, 110)

	sequence, err := LongestOplogSequenceFrom(
		[]models.Archive{first, second}, models.Timestamp{TS: 100}, true)

	require.NoError(t, err)
	require.Equal(t, Sequence{second}, sequence)
}

func TestLongestOplogSequenceFromChoosesFarthestReachableBranch(t *testing.T) {
	short := followTestArchive(t, 90, 105)
	longFirst := followTestArchive(t, 90, 100)
	longSecond := followTestArchive(t, 100, 110)

	sequence, err := LongestOplogSequenceFrom(
		[]models.Archive{short, longSecond, longFirst}, models.Timestamp{TS: 95}, false)

	require.NoError(t, err)
	require.Equal(t, Sequence{longFirst, longSecond}, sequence)
}

func TestLongestOplogSequenceFromReturnsNoDisconnectedArchives(t *testing.T) {
	disconnected := followTestArchive(t, 110, 120)

	sequence, err := LongestOplogSequenceFrom(
		[]models.Archive{disconnected}, models.Timestamp{TS: 100}, true)

	require.NoError(t, err)
	require.Empty(t, sequence)
}

func TestLongestOplogSequenceFromRejectsDuplicateEnds(t *testing.T) {
	left := followTestArchive(t, 90, 110)
	right := followTestArchive(t, 100, 110)

	_, err := LongestOplogSequenceFrom(
		[]models.Archive{left, right}, models.Timestamp{TS: 100}, true)

	require.ErrorContains(t, err, "duplicate oplog archives with end timestamp")
}

func followTestArchive(t *testing.T, start, end uint32) models.Archive {
	t.Helper()
	arch, err := models.NewArchive(
		models.Timestamp{TS: start}, models.Timestamp{TS: end}, "lz4", models.ArchiveTypeOplog)
	require.NoError(t, err)
	return arch
}
