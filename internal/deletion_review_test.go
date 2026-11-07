package internal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func TestProtectionLookupEmptyCatalog(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		t.Run(fmt.Sprint(orphan), func(t *testing.T) {
			root := memory.NewFolder("", memory.NewKVS())
			if orphan {
				recoveryPut(t, root, utility.BaseBackupPath+"orphan/payload", "data")
			}
			backups, wals, err := postgres.GetPermanentBackupsAndWalsChecked(t.Context(), root)
			require.NoError(t, err)
			require.NotNil(t, backups)
			require.Empty(t, backups)
			require.NotNil(t, wals)
			require.Empty(t, wals)
			_, err = mysql.NewDeleteHandler(t.Context(), root)
			require.NoError(t, err)
			_, err = greenplum.NewDeleteHandler(t.Context(), root, greenplum.DeleteArgs{})
			require.NoError(t, err)
			pg, err := postgres.NewDeleteHandler(t.Context(), root, backups, wals, false)
			require.NoError(t, err)
			recoveryPut(t, root, utility.WalPath+"000000010000000000000001", "wal")
			require.NoError(t, pg.HandleDeleteGarbage(t.Context(), nil, true, false))
			recoveryExists(t, root, utility.WalPath+"000000010000000000000001", true)
			require.NoError(t, pg.HandleDeleteGarbage(t.Context(), nil, false, true))
			recoveryExists(t, root, utility.WalPath+"000000010000000000000001", true)
			require.NoError(t, pg.HandleDeleteGarbage(t.Context(), nil, true, true))
			recoveryExists(t, root, utility.WalPath+"000000010000000000000001", false)
			if orphan {
				recoveryExists(t, root, utility.BaseBackupPath+"orphan/payload", false)
			}
		})
	}
}

type protectionListingFailure struct {
	storage.Folder
	err error
}

func (f protectionListingFailure) GetSubFolder(name string) storage.Folder {
	return protectionListingFailure{f.Folder.GetSubFolder(name), f.err}
}
func (f protectionListingFailure) ListFolder(context.Context) ([]storage.Object, []storage.Folder, error) {
	return nil, nil, f.err
}
func TestProtectionLookupPreservesListingFailure(t *testing.T) {
	for _, failure := range []error{errors.New("listing failed"), context.Canceled} {
		root := protectionListingFailure{memory.NewFolder("", memory.NewKVS()), failure}
		_, _, err := postgres.GetPermanentBackupsAndWalsChecked(t.Context(), root)
		require.ErrorIs(t, err, failure)
		_, err = internal.GetPermanentBackupsChecked(t.Context(), root, postgres.NewGenericMetaFetcher(), false)
		require.ErrorIs(t, err, failure)
	}
}

func TestMarkRepairsV2ProtectionConflict(t *testing.T) {
	for _, desired := range []bool{false, true} {
		t.Run(fmt.Sprint(desired), func(t *testing.T) {
			root := memory.NewFolder("", memory.NewKVS())
			name := putPGRecovery(t, root, 1, desired)
			base := root.GetSubFolder(utility.BaseBackupPath)
			start := uint64(postgres.WalSegmentSize) + 1
			recoveryPut(t, base, name+utility.SentinelSuffix, fmt.Sprintf(`{"Version":2,"LSN":%d,"FinishLSN":%d,"IsPermanent":%t,"future_field":{"keep":true}}`, start, start+5, !desired))
			_, _, err := postgres.GetPermanentBackupsAndWalsChecked(t.Context(), root)
			require.Error(t, err)
			interactor := postgres.NewGenericMetaInteractor()
			handler := internal.NewBackupMarkHandler(interactor, root)
			names, err := handler.GetBackupsToMark(t.Context(), name, desired)
			require.NoError(t, err)
			require.Equal(t, []string{name}, names)
			for _, n := range names {
				require.NoError(t, interactor.SetIsPermanent(t.Context(), n, base, desired))
			}
			doc, err := internal.ReadDeletionMetadata(t.Context(), base, name+utility.SentinelSuffix, nil)
			require.NoError(t, err)
			require.NoError(t, doc.Problem)
			marked, known := doc.Bool("IsPermanent")
			require.True(t, known)
			require.Equal(t, desired, marked)
			require.JSONEq(t, `{"keep":true}`, string(doc.Fields["future_field"]))
			_, _, err = postgres.GetPermanentBackupsAndWalsChecked(t.Context(), root)
			require.NoError(t, err)
			names, err = handler.GetBackupsToMark(t.Context(), name, desired)
			require.NoError(t, err)
			require.Empty(t, names)
		})
	}
}

func TestUserDataDoesNotResolveProtectionConflict(t *testing.T) {
	root := memory.NewFolder("", memory.NewKVS())
	name := putPGRecovery(t, root, 1, false)
	base := root.GetSubFolder(utility.BaseBackupPath)
	recoveryPut(t, base, name+utility.SentinelSuffix, `{"Version":2,"IsPermanent":true}`)
	require.NoError(t, postgres.NewGenericMetaSetter().SetUserData(t.Context(), name, base, "changed"))
	doc, err := internal.ReadDeletionMetadata(t.Context(), base, name+utility.SentinelSuffix, nil)
	require.NoError(t, err)
	permanent, known := doc.Bool("IsPermanent")
	require.True(t, known)
	require.True(t, permanent)
}

func putReviewV2(t *testing.T, root storage.Folder, number int, compact, sentinel bool, parent string) string {
	t.Helper()
	name := putPGRecovery(t, root, number, compact)
	start := uint64(number)*postgres.WalSegmentSize + 1
	fields := map[string]any{"Version": 2, "LSN": start, "FinishLSN": start + 5, "IsPermanent": sentinel}
	if parent != "" {
		fields["DeltaFrom"] = parent
		fields["DeltaFullName"] = parent
		fields["DeltaCount"] = 1
		fields["DeltaLSN"] = uint64(postgres.WalSegmentSize) + 1
	}
	raw, err := json.Marshal(fields)
	require.NoError(t, err)
	recoveryPut(t, root.GetSubFolder(utility.BaseBackupPath), name+utility.SentinelSuffix, string(raw))
	return name
}
func TestMarkRepairsAncestorsAndProtectsV2Dependents(t *testing.T) {
	root := memory.NewFolder("", memory.NewKVS())
	parent := putReviewV2(t, root, 1, true, false, "")
	child := putReviewV2(t, root, 2, true, true, parent)
	marker := internal.NewBackupMarkHandler(postgres.NewGenericMetaInteractor(), root)
	names, err := marker.GetBackupsToMark(t.Context(), child, true)
	require.NoError(t, err)
	require.Equal(t, []string{parent}, names)
	require.NoError(t, postgres.NewGenericMetaSetter().SetIsPermanent(t.Context(), parent, root.GetSubFolder(utility.BaseBackupPath), true))
	putReviewV2(t, root, 2, false, true, parent)
	_, err = marker.GetBackupsToMark(t.Context(), parent, false)
	require.Error(t, err)
}

type markWriteFailure struct {
	storage.Folder
	key    string
	commit bool
	writes *[]string
}

func (f markWriteFailure) PutObject(ctx context.Context, key string, r io.Reader) error {
	*f.writes = append(*f.writes, key)
	if key == f.key {
		if f.commit {
			if err := f.Folder.PutObject(ctx, key, r); err != nil {
				return err
			}
		}
		return errors.New("injected write failure")
	}
	return f.Folder.PutObject(ctx, key, r)
}
func TestV2MarkRetriesAfterWriteFailure(t *testing.T) {
	for _, desired := range []bool{false, true} {
		for _, part := range []string{"sentinel", "compact"} {
			for _, commit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%t/%s/%t", desired, part, commit), func(t *testing.T) {
					root := memory.NewFolder("", memory.NewKVS())
					name := putReviewV2(t, root, 1, !desired, !desired, "")
					base := root.GetSubFolder(utility.BaseBackupPath)
					key := name + utility.SentinelSuffix
					if part == "compact" {
						key = name + "/metadata.json"
					}
					var writes []string
					err := postgres.NewGenericMetaSetter().SetIsPermanent(t.Context(), name, markWriteFailure{base, key, commit, &writes}, desired)
					require.ErrorContains(t, err, "injected write failure")
					if part == "sentinel" {
						require.Equal(t, []string{key}, writes)
					}
					marker := internal.NewBackupMarkHandler(postgres.NewGenericMetaInteractor(), root)
					names, err := marker.GetBackupsToMark(t.Context(), name, desired)
					require.NoError(t, err)
					for _, n := range names {
						require.NoError(t, postgres.NewGenericMetaSetter().SetIsPermanent(t.Context(), n, base, desired))
					}
					b, err := postgres.LoadDeletionBackup(t.Context(), root, name)
					require.NoError(t, err)
					require.NoError(t, b.Problem)
					require.Equal(t, desired, b.Permanent)
				})
			}
		}
	}
}
