package models

import (
	"reflect"
	"testing"
)

func TestNewArchive(t *testing.T) {
	type args struct {
		start Timestamp
		end   Timestamp
		ext   string
	}
	tests := []struct {
		name    string
		args    args
		want    Archive
		wantErr bool
	}{
		{
			name: "start > end",
			args: args{
				start: Timestamp{1579541543, 32},
				end:   Timestamp{1579541443, 33},
				ext:   "lzo",
			},
			wantErr: true,
		},
		{
			name: "start end ext",
			args: args{
				start: Timestamp{1579541143, 39},
				end:   Timestamp{1579541443, 33},
				ext:   "lzo",
			},
			want: Archive{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewArchive(tt.args.start, tt.args.end, tt.args.ext)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewArchive() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewArchive() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArchive_In(t *testing.T) {
	type fields struct {
		Start Timestamp
		End   Timestamp
		Ext   string
	}
	type args struct {
		ts Timestamp
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "TS is not in archive, left",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			args: args{
				ts: Timestamp{1579540143, 99},
			},
			want: false,
		},
		{
			name: "TS is not in archive, right",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			args: args{
				ts: Timestamp{1579549143, 7},
			},
			want: false,
		},
		{
			name: "TS is in archive, middle",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			args: args{
				ts: Timestamp{1579541143, 47},
			},
			want: true,
		},
		{
			name: "TS is in archive, left",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			args: args{
				ts: Timestamp{1579541143, 39},
			},
			want: false,
		},
		{
			name: "TS is in archive, right",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			args: args{
				ts: Timestamp{1579541443, 33},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Archive{
				Start: tt.fields.Start,
				End:   tt.fields.End,
				Ext:   tt.fields.Ext,
			}
			if got := a.In(tt.args.ts); got != tt.want {
				t.Errorf("In() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArchive_Filename(t *testing.T) {
	type fields struct {
		Start Timestamp
		End   Timestamp
		Ext   string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Archive, lzo",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			want: "oplog_1579541143.39_1579541443.33.lzo",
		},
		{
			name: "Archive, zip",
			fields: fields{
				Start: Timestamp{1579543687, 1},
				End:   Timestamp{1579543705, 2},
				Ext:   "zip",
			},
			want: "oplog_1579543687.1_1579543705.2.zip",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Archive{
				Start: tt.fields.Start,
				End:   tt.fields.End,
				Ext:   tt.fields.Ext,
			}
			if got := a.Filename(); got != tt.want {
				t.Errorf("Filename() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArchFromFilename(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    Archive
		wantErr bool
	}{
		{
			name: "Filename with lzo",
			args: args{
				path: "oplog_1579541143.39_1579541443.33.lzo",
			},
			want: Archive{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
			},
			wantErr: false,
		},
		{
			name: "Filename with zip",
			args: args{
				path: "oplog_1579541143.1_1579541443.2.zip",
			},
			want: Archive{
				Start: Timestamp{1579541143, 1},
				End:   Timestamp{1579541443, 2},
				Ext:   "zip",
			},
			wantErr: false,
		},
		{
			name: "Filename without prefix",
			args: args{
				path: "1579541143.1_1579541443.2.zip",
			},
			wantErr: true,
		},
		{
			name: "Filename without extension",
			args: args{
				path: "oplog_1579541143.39_1579541443.33",
			},
			wantErr: true,
		},
		{
			name: "Filename without second TS",
			args: args{
				path: "oplog_1579541143.39",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ArchFromFilename(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ArchFromFilename() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ArchFromFilename() got = %v, want %v", got, tt.want)
			}
		})
	}
}
