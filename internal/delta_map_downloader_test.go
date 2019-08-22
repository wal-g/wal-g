package internal

import (
	"testing"
)

func TestGetDeltaRange(t *testing.T) {
	deltaNo000000010000011100000000 := NewDeltaNoFromFilenameNoError("000000010000011100000000")
	deltaNo000000010000011100000010 := deltaNo000000010000011100000000.Next()
	deltaNo000000010000011100000020 := deltaNo000000010000011100000010.Next()
	deltaNo000000010000011100000030 := deltaNo000000010000011100000020.Next()

	walSegNo000000010000011100000014 := NewWalSegmentNoFromFilenameNoError("000000010000011100000014")
	walSegNo000000010000011100000015 := walSegNo000000010000011100000014.Next()
	walSegNo000000010000011100000009 := deltaNo000000010000011100000010.FirstWalSegmentNo().Previous()
	type args struct {
		firstUsedLsn    uint64
		firstNotUsedLsn uint64
	}
	tests := []struct {
		name                string
		args                args
		firstUsedDeltaNo    DeltaNo
		firstNotUsedDeltaNo DeltaNo
	}{
		{"firstUsedLsn = firstNotUsedLsn",
			args{
				walSegNo000000010000011100000014.FirstLsn(),
				walSegNo000000010000011100000014.FirstLsn(),
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000010,
		},
		{"firstUsedLsn > firstNotUsedLsn from different deltas",
			args{
				deltaNo000000010000011100000010.FirstLsn(),
				walSegNo000000010000011100000009.FirstLsn(),
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000000,
		},
		{"firstUsedLsn and firstNotUsedLsn from the same WAL Segment",
			args{
				walSegNo000000010000011100000014.FirstLsn(),
				walSegNo000000010000011100000015.FirstLsn() - 1,
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000010,
		},
		{"firstUsedLsn and firstNotUsedLsn from the same delta file",
			args{
				deltaNo000000010000011100000010.FirstLsn(),
				deltaNo000000010000011100000020.FirstLsn() - 1,
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000010,
		},
		{"firstNotUsedLsn is first Lsn from next delta file",
			args{
				walSegNo000000010000011100000014.FirstLsn(),
				deltaNo000000010000011100000020.FirstLsn(),
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000020,
		},
		{"firstNotUsedLsn is last Lsn from next delta file",
			args{
				deltaNo000000010000011100000010.FirstLsn(),
				deltaNo000000010000011100000030.FirstLsn() - 1,
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000020,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetDeltaRange(tt.args.firstUsedLsn, tt.args.firstNotUsedLsn)
			if got != tt.firstUsedDeltaNo {
				t.Errorf("GetDeltaRange() got = %v, firstUsedDeltaNo %v", got, tt.firstUsedDeltaNo)
			}
			if got1 != tt.firstNotUsedDeltaNo {
				t.Errorf("GetDeltaRange() got1 = %v, firstUsedDeltaNo %v", got1, tt.firstNotUsedDeltaNo)
			}
		})
	}
}

func TestGetWalSegmentRange(t *testing.T) {
	deltaNo000000010000011100000009 := NewDeltaNoFromFilenameNoError("000000010000011100000009")
	type args struct {
		firstNotUsedDeltaNo DeltaNo
		firstNotUsedLsn     uint64
	}
	tests := []struct {
		name  string
		args  args
		want  WalSegmentNo
		want1 WalSegmentNo
	}{
		{"firstNotUsedLsn is first Lsn in a WAL segment",
			args{
				deltaNo000000010000011100000009,
				deltaNo000000010000011100000009.FirstWalSegmentNo().Next().FirstLsn(),
			},
			deltaNo000000010000011100000009.FirstWalSegmentNo(),
			deltaNo000000010000011100000009.FirstWalSegmentNo().Next(),
		},
		{"firstNotUsedLsn is second Lsn in a WAL segment",
			args{
				deltaNo000000010000011100000009,
				deltaNo000000010000011100000009.FirstWalSegmentNo().Next().FirstLsn() + 1,
			},
			deltaNo000000010000011100000009.FirstWalSegmentNo(),
			deltaNo000000010000011100000009.FirstWalSegmentNo().Next().Next(),
		},
		{"firstNotUsedLsn in the same WAL segment that is first WAL segment of firstNotUsedDeltaNo",
			args{
				deltaNo000000010000011100000009,
				deltaNo000000010000011100000009.FirstWalSegmentNo().FirstLsn(),
			},
			deltaNo000000010000011100000009.FirstWalSegmentNo(),
			deltaNo000000010000011100000009.FirstWalSegmentNo(),},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetWalSegmentRange(tt.args.firstNotUsedDeltaNo, tt.args.firstNotUsedLsn)
			if got != tt.want {
				t.Errorf("GetWalSegmentRange() got = %v, firstUsedDeltaNo %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetWalSegmentRange() got1 = %v, firstUsedDeltaNo %v", got1, tt.want1)
			}
		})
	}
}
