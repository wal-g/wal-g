package models

import (
	"reflect"
	"testing"
)

func TestNewArchive(t *testing.T) {
	type args struct {
		Start Timestamp
		End   Timestamp
		Ext   string
		Type  string
	}
	tests := []struct {
		name    string
		args    args
		want    Archive
		wantErr bool
	}{
		{
			name: "unknown_type",
			args: args{
				Start: Timestamp{1579541143, 32},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "type",
			},
			wantErr: true,
		},
		{
			name: "start_>_End",
			args: args{
				Start: Timestamp{1579541543, 32},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "oplog",
			},
			wantErr: true,
		},
		{
			name: "start_end_ext_type",
			args: args{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "oplog",
			},
			want: Archive{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "oplog",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewArchive(tt.args.Start, tt.args.End, tt.args.Ext, tt.args.Type)
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
		Type  string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "archive,_lzo",
			fields: fields{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "oplog",
			},
			want: "oplog_1579541143.39_1579541443.33.lzo",
		},
		{
			name: "archive,_zip",
			fields: fields{
				Start: Timestamp{1579543687, 1},
				End:   Timestamp{1579543705, 2},
				Ext:   "zip",
				Type:  "gap",
			},
			want: "gap_1579543687.1_1579543705.2.zip",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Archive{
				Start: tt.fields.Start,
				End:   tt.fields.End,
				Ext:   tt.fields.Ext,
				Type:  tt.fields.Type,
			}
			if got := a.Filename(); got != tt.want {
				t.Errorf("OplogFilename() = %v, want %v", got, tt.want)
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
			name: "archive_lzo",
			args: args{
				path: "oplog_1579541143.39_1579541443.33.lzo",
			},
			want: Archive{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "oplog",
			},
			wantErr: false,
		},
		{
			name: "archive_zip",
			args: args{
				path: "oplog_1579541143.1_1579541443.2.zip",
			},
			want: Archive{
				Start: Timestamp{1579541143, 1},
				End:   Timestamp{1579541443, 2},
				Ext:   "zip",
				Type:  "oplog",
			},
			wantErr: false,
		},
		{
			name: "archive_gap",
			args: args{
				path: "gap_1579541143.39_1579541443.33.lzo",
			},
			want: Archive{
				Start: Timestamp{1579541143, 39},
				End:   Timestamp{1579541443, 33},
				Ext:   "lzo",
				Type:  "gap",
			},
			wantErr: false,
		},
		{
			name: "archive_without_prefix",
			args: args{
				path: "1579541143.1_1579541443.2.zip",
			},
			wantErr: true,
		},
		{
			name: "archive_without_extension",
			args: args{
				path: "oplog_1579541143.39_1579541443.33",
			},
			wantErr: true,
		},
		{
			name: "archive_without_second_TS",
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
