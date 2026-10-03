package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRunnerForDatabaseReusesClusterInfo(t *testing.T) {
	systemIdentifier := uint64(123456789)
	queryRunner := &PgQueryRunner{
		Version:          180000,
		SystemIdentifier: &systemIdentifier,
	}

	databaseRunner, err := queryRunner.newRunnerForDatabase(nil)
	require.NoError(t, err)
	require.Equal(t, queryRunner.Version, databaseRunner.Version)
	require.Same(t, queryRunner.SystemIdentifier, databaseRunner.SystemIdentifier)
}

func TestNewRunnerForDatabasePreservesMissingSystemIdentifier(t *testing.T) {
	queryRunner := &PgQueryRunner{Version: 180006}

	// A database runner inherits missing cluster metadata without trying to
	// query it on its own connection.
	databaseRunner, err := queryRunner.newRunnerForDatabase(nil)
	require.NoError(t, err)
	require.Equal(t, queryRunner.Version, databaseRunner.Version)
	require.Nil(t, databaseRunner.SystemIdentifier)
}

func TestInitQueryRunnerReusesExistingRunner(t *testing.T) {
	systemIdentifier := uint64(123456789)
	runner := &PgQueryRunner{
		Version:          180006,
		SystemIdentifier: &systemIdentifier,
	}
	handler := &BackupHandler{Workers: BackupWorkers{QueryRunner: runner}}

	// A nil connection makes any accidental metadata query fail. The existing
	// runner must be reused without connecting or querying the cluster again.
	require.NoError(t, handler.initQueryRunner(context.Background()))
	require.Same(t, runner, handler.Workers.QueryRunner)
	require.Equal(t, 180006, handler.Workers.QueryRunner.Version)
	require.Same(t, &systemIdentifier, handler.Workers.QueryRunner.SystemIdentifier)
}
