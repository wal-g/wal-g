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

	databaseRunner, err := queryRunner.newRunnerForDatabase(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, queryRunner.Version, databaseRunner.Version)
	require.Same(t, queryRunner.SystemIdentifier, databaseRunner.SystemIdentifier)
}
