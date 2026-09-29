package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckWalSummaryCoverage(t *testing.T) {
	summaries := []walSummary{{0x100, 0x200}, {0x200, 0x300}, {0x280, 0x400}}
	assert.NoError(t, checkWalSummaryCoverage(summaries, 0x150, 0x350))
	assert.NoError(t, checkWalSummaryCoverage(summaries, 0x100, 0x400))
	assert.Error(t, checkWalSummaryCoverage(summaries, 0x50, 0x150), "head missing")
	assert.Error(t, checkWalSummaryCoverage(summaries, 0x150, 0x500), "tail missing")
	assert.Error(t, checkWalSummaryCoverage([]walSummary{{0x100, 0x200}, {0x280, 0x300}}, 0x150, 0x300), "gap")
	assert.Error(t, checkWalSummaryCoverage(nil, 0x100, 0x200), "none")
}
