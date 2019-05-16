package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"os"
	"testing"
)

func TestConfigurePreventWalOverwrite_CorrectEnvVariable(t *testing.T) {
	os.Setenv("WALG_PREVENT_WAL_OVERWRITE", "true")
	preventWalOverwrite, err := internal.ConfigurePreventWalOverwrite()
	assert.NoError(t, err)
	assert.Equal(t, true, preventWalOverwrite)
	os.Unsetenv("WALG_PREVENT_WAL_OVERWRITE")
}

func TestConfigurePreventWalOverwrite_IncorrectEnvVariable(t *testing.T) {
	os.Setenv("WALG_PREVENT_WAL_OVERWRITE", "fail")
	_, err := internal.ConfigurePreventWalOverwrite()
	assert.Error(t, err)
	os.Unsetenv("WALG_PREVENT_WAL_OVERWRITE")
}
