package printlist

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	entities := []Entity{
		shortEntity,
		longEntity,
		emptyEntity,
	}

	tests := []struct {
		name         string
		entities     []Entity
		pretty, json bool
		wantOutput   string
		wantErr      assert.ErrorAssertionFunc
	}{
		{
			name:       "print plain json",
			entities:   entities,
			pretty:     false,
			json:       true,
			wantOutput: fmt.Sprintf("[%s,%s,%s]\n", shortEntityPlainJSON, longEntityPlainJSON, emptyEntityPlainJSON),
			wantErr:    assert.NoError,
		},
		{
			name:       "print indented json",
			entities:   entities,
			pretty:     true,
			json:       true,
			wantOutput: fmt.Sprintf("[\n%s,\n%s,\n%s\n]\n", shortEntityIndentedJSON, longEntityIndentedJSON, emptyEntityIndentedJSON),
			wantErr:    assert.NoError,
		},
		{
			name:       "print empty json",
			entities:   []Entity{},
			pretty:     true,
			json:       true,
			wantOutput: "[]\n",
			wantErr:    assert.NoError,
		},
		{
			name:     "print ascii table",
			entities: entities,
			pretty:   true,
			json:     false,
			wantOutput: fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n",
				testEntityASCIIHeader,
				shortEntityASCIIRow,
				longEntityASCIIRow,
				emptyEntityASCIIRow,
				testEntityASCIISeparator,
			),
			wantErr: assert.NoError,
		},
		{
			name:       "print empty ascii table",
			entities:   []Entity{},
			pretty:     true,
			json:       false,
			wantOutput: "",
			wantErr:    assert.NoError,
		},
		{
			name:     "print tabbed table",
			entities: entities,
			pretty:   false,
			json:     false,
			wantOutput: fmt.Sprintf("%s\n%s\n%s\n%s\n",
				testEntityTabbedHeader,
				shortEntityTabbedRow,
				longEntityTabbedRow,
				emptyEntityTabbedRow,
			),
			wantErr: assert.NoError,
		},
		{
			name:       "print empty tabbed table",
			entities:   []Entity{},
			pretty:     false,
			json:       false,
			wantOutput: "",
			wantErr:    assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := new(bytes.Buffer)
			err := List(tt.entities, output, tt.pretty, tt.json)
			t.Logf("actual output:\n%s<end>", output)
			if !tt.wantErr(t, err) {
				return
			}
			assert.Equal(t, tt.wantOutput, output.String())
		})
	}
}

type testEntity struct {
	A         string `json:"a"`
	APretty   string `json:"-"`
	B         string `json:"b"`
	CJSONOnly string `json:"c,omitempty"`
}

func (e testEntity) PrintableFields() []TableField {
	return []TableField{
		{
			Name:        "a",
			PrettyName:  "A (pretty)",
			Value:       e.A,
			PrettyValue: &e.APretty,
		},
		{
			Name:        "b",
			PrettyName:  "B (pretty)",
			Value:       e.B,
			PrettyValue: nil,
		},
	}
}

var (
	testEntityASCIISeparator = "+---+----------------------------------------------------------+------------+"

	testEntityASCIIHeader = strings.Join(
		[]string{
			testEntityASCIISeparator,
			"| # | A (PRETTY)                                               | B (PRETTY) |",
			testEntityASCIISeparator,
		},
		"\n",
	)

	testEntityTabbedHeader = "a                                                      b"
)

var (
	shortEntity = testEntity{
		A:         "1692753495",
		APretty:   "Thu, 23 Aug 2023 09:26:25",
		B:         "123",
		CJSONOnly: "kek",
	}

	shortEntityPlainJSON = `{"a":"1692753495","b":"123","c":"kek"}`

	shortEntityIndentedJSON = `    {
        "a": "1692753495",
        "b": "123",
        "c": "kek"
    }`

	shortEntityASCIIRow = `| 0 | Thu, 23 Aug 2023 09:26:25                                | 123        |`

	shortEntityTabbedRow = `1692753495                                             123`
)

var (
	longValue       = "lorem ipsum dolor sit amet consectetur adipiscing elit"
	prettyLongValue = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."

	longEntity = testEntity{
		A:         longValue,
		APretty:   prettyLongValue,
		B:         "some value",
		CJSONOnly: "details",
	}

	longEntityPlainJSON = `{"a":"lorem ipsum dolor sit amet consectetur adipiscing elit","b":"some value","c":"details"}`

	longEntityIndentedJSON = `    {
        "a": "lorem ipsum dolor sit amet consectetur adipiscing elit",
        "b": "some value",
        "c": "details"
    }`

	longEntityASCIIRow = `| 1 | Lorem ipsum dolor sit amet, consectetur adipiscing elit. | some value |`

	longEntityTabbedRow = `lorem ipsum dolor sit amet consectetur adipiscing elit some value`
)

var (
	emptyEntity = testEntity{
		A:         "",
		APretty:   "",
		B:         "",
		CJSONOnly: "",
	}

	emptyEntityPlainJSON = `{"a":"","b":""}`

	emptyEntityIndentedJSON = `    {
        "a": "",
        "b": ""
    }`

	emptyEntityASCIIRow = `| 2 |                                                          |            |`

	emptyEntityTabbedRow = `                                                       `
)
