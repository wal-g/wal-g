package models

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestLess(t *testing.T) {
	type args struct {
		ots1 Timestamp
		ots2 Timestamp
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "First ts < second ts",
			args: args{
				ots1: Timestamp{1579540093, 1},
				ots2: Timestamp{1579540193, 1},
			},
			want: true,
		},
		{
			name: "First ts > second ts",
			args: args{
				ots1: Timestamp{1579540193, 42},
				ots2: Timestamp{1579540093, 42},
			},
			want: false,
		},
		{
			name: "First inc < second inc, ts are equal",
			args: args{
				ots1: Timestamp{1579540193, 0},
				ots2: Timestamp{1579540193, 1},
			},
			want: true,
		},
		{
			name: "First inc > second inc, ts are equal",
			args: args{
				ots1: Timestamp{1579540193, 101},
				ots2: Timestamp{1579540193, 42},
			},
			want: false,
		},
		{
			name: "First ts < second ts, first inc > second inc",
			args: args{
				ots1: Timestamp{1579540193, 42},
				ots2: Timestamp{1579540393, 13},
			},
			want: true,
		},
		{
			name: "First ts < second ts, first inc > second inc",
			args: args{
				ots1: Timestamp{1579541193, 3},
				ots2: Timestamp{1579540393, 13},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LessTS(tt.args.ots1, tt.args.ots2); got != tt.want {
				t.Errorf("LessTS() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMax(t *testing.T) {
	type args struct {
		ots1 Timestamp
		ots2 Timestamp
	}
	tests := []struct {
		name string
		args args
		want Timestamp
	}{
		{
			name: "First < second",
			args: args{
				ots1: Timestamp{1579540093, 1},
				ots2: Timestamp{1579540193, 1},
			},
			want: Timestamp{1579540193, 1},
		},
		{
			name: "First > second",
			args: args{
				ots1: Timestamp{1579540193, 42},
				ots2: Timestamp{1579540093, 42},
			},
			want: Timestamp{1579540193, 42},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaxTS(tt.args.ots1, tt.args.ots2); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MaxTS() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestampFromBson(t *testing.T) {
	type args struct {
		bts primitive.Timestamp
	}
	tests := []struct {
		name string
		args args
		want Timestamp
	}{
		{
			name: "Zero TS",
			args: args{
				bts: primitive.Timestamp{T: 0, I: 0},
			},
			want: Timestamp{0, 0},
		},
		{
			name: "TS",
			args: args{
				bts: primitive.Timestamp{T: 1579541242, I: 0},
			},
			want: Timestamp{1579541242, 0},
		},
		{
			name: "TS inc",
			args: args{
				bts: primitive.Timestamp{T: 1579541342, I: 11},
			},
			want: Timestamp{1579541342, 11},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimestampFromBson(tt.args.bts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimestampFromBson() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBsonTimestampFromOplogTS(t *testing.T) {
	type args struct {
		ots Timestamp
	}
	tests := []struct {
		name string
		args args
		want primitive.Timestamp
	}{
		{
			name: "Zero TS",
			args: args{
				ots: Timestamp{0, 0},
			},
			want: primitive.Timestamp{T: 0, I: 0},
		},
		{
			name: "TS",
			args: args{
				ots: Timestamp{1579541242, 0},
			},
			want: primitive.Timestamp{T: 1579541242, I: 0},
		},
		{
			name: "TS inc",
			args: args{
				ots: Timestamp{1579541342, 11},
			},
			want: primitive.Timestamp{T: 1579541342, I: 11},
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BsonTimestampFromOplogTS(tt.args.ots); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BsonTimestampFromOplogTS() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestamp_String(t *testing.T) {
	type fields struct {
		TS  uint32
		Inc uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "Zero TS",
			fields: fields{0, 0},
			want:   "0.0",
		},
		{
			name:   "TS",
			fields: fields{1579541443, 32},
			want:   "1579541443.32",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ots := Timestamp{
				TS:  tt.fields.TS,
				Inc: tt.fields.Inc,
			}
			if got := ots.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestampFromStr(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    Timestamp
		wantErr bool
	}{
		{
			name: "TS, zero INC",
			args: args{
				s: "1579541242.0",
			},
			want:    Timestamp{1579541242, 0},
			wantErr: false,
		},
		{
			name: "TS, INC",
			args: args{
				s: "1579541242.142",
			},
			want:    Timestamp{1579541242, 142},
			wantErr: false,
		},
		{
			name: "TS err, INC",
			args: args{
				s: "15795412a2.142",
			},
			wantErr: true,
		},
		{
			name: "TS, INC err",
			args: args{
				s: "1579541242.1a2",
			},
			wantErr: true,
		},
		{
			name: "No separator",
			args: args{
				s: "157954124212",
			},
			wantErr: true,
		},
		{
			name: "String",
			args: args{
				s: "string",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TimestampFromStr(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("TimestampFromStr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimestampFromStr() got = %v, want %v", got, tt.want)
			}
		})
	}
}
