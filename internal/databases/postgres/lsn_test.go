package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func TestParseLSNOrNumber(t *testing.T) {
	tests := []struct {
		input   string
		want    postgres.LSN
		wantErr bool
	}{
		{input: "0/EDA81980", want: 3987216768},
		{input: "0/eda81980", want: 3987216768},
		{input: "3987216768", want: 3987216768},
		{input: "1/0", want: 1 << 32},
		{input: "0/0", want: 0},
		{input: "0", want: 0},
		{input: "", wantErr: true},
		{input: "EDA81980", wantErr: true},
		{input: "-1", wantErr: true},
		{input: "0/XYZ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := postgres.ParseLSNOrNumber(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
