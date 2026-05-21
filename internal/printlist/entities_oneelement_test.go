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
			name:       "print plain json",
			entity:     entity,
			pretty:     false,
			json:       true,
			wantOutput: fmt.Sprintf("%s\n", shortEntityPlainJSONLocal),
			wantErr:    assert.NoError,
		},
		{
			name:       "print indented json",
			entity:     entity,
			pretty:     true,
			json:       true,
			wantOutput: fmt.Sprintf("%s\n", shortEntityIndentedJSONLocal),
			wantErr:    assert.NoError,
		},
		{
			name:       "not json format",
			entity:     entity,
			pretty:     false,
			json:       false,
			wantOutput: "",
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
			wantOutput: fmt.Sprintf("%s\n", shortEntityPlainJSONLocal),
			wantErr:    assert.NoError,
		},
		{
			name:       "indented json",
			entity:     entity,
			pretty:     true,
			wantOutput: fmt.Sprintf("%s\n", shortEntityIndentedJSONLocal),
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

// These variables are defined in entities_test.go but we need them here too
// Using different names to avoid conflicts
var (
	shortEntityPlainJSONLocal = `{"a":"1692753495","b":"123","c":"kek"}`

	shortEntityIndentedJSONLocal = `{
    "a": "1692753495",
    "b": "123",
    "c": "kek"
}`
)
