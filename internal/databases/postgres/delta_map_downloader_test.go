package postgres

import (
	"testing"
)

func TestGetDeltaRange(t *testing.T) {
	deltaNo000000010000011100000000, _ := newDeltaNoFromFilename("000000010000011100000000")
	deltaNo000000010000011100000010 := deltaNo000000010000011100000000.next()
	deltaNo000000010000011100000020 := deltaNo000000010000011100000010.next()
	deltaNo000000010000011100000030 := deltaNo000000010000011100000020.next()

	walSegNo000000010000011100000014, _ := newWalSegmentNoFromFilename("000000010000011100000014")
	walSegNo000000010000011100000015 := walSegNo000000010000011100000014.next()
	walSegNo000000010000011100000009 := deltaNo000000010000011100000010.firstWalSegmentNo().previous()
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
				walSegNo000000010000011100000014.firstLsn(),
				walSegNo000000010000011100000014.firstLsn(),
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000010,
		},
		{"firstUsedLsn > firstNotUsedLsn from different deltas",
			args{
				deltaNo000000010000011100000010.firstLsn(),
				walSegNo000000010000011100000009.firstLsn(),
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000000,
		},
		{"firstUsedLsn and firstNotUsedLsn from the same WAL Segment",
			args{
				walSegNo000000010000011100000014.firstLsn(),
				walSegNo000000010000011100000015.firstLsn() - 1,
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000010,
		},
		{"firstUsedLsn and firstNotUsedLsn from the same delta file",
			args{
				deltaNo000000010000011100000010.firstLsn(),
				deltaNo000000010000011100000020.firstLsn() - 1,
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000010,
		},
		{"firstNotUsedLsn is first Lsn from next delta file",
			args{
				walSegNo000000010000011100000014.firstLsn(),
				deltaNo000000010000011100000020.firstLsn(),
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000020,
		},
		{"firstNotUsedLsn is last Lsn from next delta file",
			args{
				deltaNo000000010000011100000010.firstLsn(),
				deltaNo000000010000011100000030.firstLsn() - 1,
			},
			deltaNo000000010000011100000010,
			deltaNo000000010000011100000020,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getDeltaRange(tt.args.firstUsedLsn, tt.args.firstNotUsedLsn)
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
	deltaNo000000010000011100000009, _ := newDeltaNoFromFilename("000000010000011100000009")
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
				deltaNo000000010000011100000009.firstWalSegmentNo().next().firstLsn(),
			},
			deltaNo000000010000011100000009.firstWalSegmentNo(),
			deltaNo000000010000011100000009.firstWalSegmentNo().next(),
		},
		{"firstNotUsedLsn is second Lsn in a WAL segment",
			args{
				deltaNo000000010000011100000009,
				deltaNo000000010000011100000009.firstWalSegmentNo().next().firstLsn() + 1,
			},
			deltaNo000000010000011100000009.firstWalSegmentNo(),
			deltaNo000000010000011100000009.firstWalSegmentNo().next().next(),
		},
		{"firstNotUsedLsn in the same WAL segment that is first WAL segment of firstNotUsedDeltaNo",
			args{
				deltaNo000000010000011100000009,
				deltaNo000000010000011100000009.firstWalSegmentNo().firstLsn(),
			},
			deltaNo000000010000011100000009.firstWalSegmentNo(),
			deltaNo000000010000011100000009.firstWalSegmentNo()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getWalSegmentRange(tt.args.firstNotUsedDeltaNo, tt.args.firstNotUsedLsn)
			if got != tt.want {
				t.Errorf("GetWalSegmentRange() got = %v, firstUsedDeltaNo %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetWalSegmentRange() got1 = %v, firstUsedDeltaNo %v", got1, tt.want1)
			}
		})
	}
}
