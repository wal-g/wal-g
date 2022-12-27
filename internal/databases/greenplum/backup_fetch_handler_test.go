package greenplum

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/stretchr/testify/assert"
)

func TestPrepareContentIDsToFetch(t *testing.T) {
	testcases := []struct {
		fetchContentId []int
		segmentConfig []cluster.SegConfig
		contentIDsToFetch map[int]bool
	} {
		{
			fetchContentId: []int{},
			segmentConfig: []cluster.SegConfig{},
			contentIDsToFetch: map[int]bool{},
		},
		{
			fetchContentId: []int{},
			segmentConfig: []cluster.SegConfig{{ContentID: 21}, {ContentID: 42}},
			contentIDsToFetch: map[int]bool{21: true, 42: true},
		},
		{
			fetchContentId: []int{1},
			segmentConfig: []cluster.SegConfig{{ContentID: 1231}, {ContentID: 6743}, {ContentID: 7643}},
			contentIDsToFetch: map[int]bool{1: true},
		},
		{
			fetchContentId: []int{65, 42, 12, 76, 22},
			segmentConfig: []cluster.SegConfig{},
			contentIDsToFetch: map[int]bool{65: true, 42: true, 12: true, 76: true, 22: true},
		},
		{
			fetchContentId: []int{5, 4, 3, 2, 1},
			segmentConfig: []cluster.SegConfig{{ContentID: 4}, {ContentID: 5}, {ContentID: 6}},
			contentIDsToFetch: map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true},
		},
		{
			fetchContentId: []int{6, 7, 8, 9, 10},
			segmentConfig: []cluster.SegConfig{{ContentID: 1}, {ContentID: 5}, {ContentID: 7}},
			contentIDsToFetch: map[int]bool{6: true, 7: true, 8: true, 9: true, 10: true},
		},
	}

	for _, tc := range testcases {
		contentIDsToFetch := prepareContentIDsToFetch(tc.fetchContentId, tc.segmentConfig)
		assert.Equal(t, tc.contentIDsToFetch, contentIDsToFetch)
	}
}
