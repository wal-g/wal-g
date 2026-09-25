package printlist

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneElement(t *testing.T) {
	entity := shortEntity

	tests := []struct {
		name         string
		entity       Entity
		pretty, json bool
		wantOutput   string
		wantErr      assert.ErrorAssertionFunc
	}{
		{
			name:       "not json format",
			entity:     entity,
			pretty:     false,
			json:       false,
			wantOutput: fmt.Sprintf("%v\n", entity),
			wantErr:    assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := new(bytes.Buffer)
			err := OneElement(tt.entity, output, tt.pretty, tt.json)
			t.Logf("actual output:\n%s<end>", output)
			if !tt.wantErr(t, err) {
				return
			}
			assert.Equal(t, tt.wantOutput, output.String())
		})
	}
}

// TestOneElementInJSON is a dedicated test for the JSON code path of OneElement
// (the oneElementInJSON helper). JSON output is verified separately from the
// non-JSON (fmt.Fprintln) path covered by TestOneElement.
func TestOneElementInJSON(t *testing.T) {
	entity := shortEntity

	tests := []struct {
		name       string
		entity     Entity
		pretty     bool
		wantOutput string
		wantErr    assert.ErrorAssertionFunc
	}{
		{
			name:       "plain json",
			entity:     entity,
			pretty:     false,
			wantOutput: fmt.Sprintf("%s\n", shortEntityPlainJSON),
			wantErr:    assert.NoError,
		},
		{
			name:       "indented json",
			entity:     entity,
			pretty:     true,
			wantOutput: fmt.Sprintf("%s\n", shortEntityOneElementIndentedJSON),
			wantErr:    assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := new(bytes.Buffer)
			err := oneElementInJSON(tt.entity, output, tt.pretty)
			t.Logf("actual output:\n%s<end>", output)
			if !tt.wantErr(t, err) {
				return
			}
			assert.Equal(t, tt.wantOutput, output.String())
		})
	}
}
