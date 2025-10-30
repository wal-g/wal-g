package archive

import (
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/archive/mocks"
)

type Addrs struct {
	addrs    []string
	lookedup bool
}

func TestGetSlotsMap(t *testing.T) {
	tests := []struct {
		name                  string
		ipAddrs               map[string]Addrs // ip -> hostnames
		valkeyFQDNToIDMap     string
		valkeyClusterConfPath string
		confContent           string
		expected              map[string][][]string
		expectedErr           error
	}{
		{
			name: "[success] simple parsing",
			ipAddrs: map[string]Addrs{
				"ip1": {[]string{"hostname1"}, true},
				"ip2": {[]string{"hostname2"}, true},
			},
			valkeyFQDNToIDMap:     `{"hostname1": "id1", "hostname2": "id2"}`,
			valkeyClusterConfPath: "test_cluster.conf",
			confContent: `
56cac18e538888e2fb81b09b8491e819d2bda1e1 ip1:6379@16379 master,nofailover - 0 1747228909000 44 connected 2731-5460 10923-13653
d36dacb40728f82b6453a611941cded23915d24a ip2:6379@16379 master,nofailover - 0 1747228909000 44 connected 5461-10922
`,
			expected: map[string][][]string{
				"id1": {{"2731", "5460"}, {"10923", "13653"}},
				"id2": {{"5461", "10922"}},
			},
		},
		{
			name: "[err] migrating slots",
			ipAddrs: map[string]Addrs{
				"ip1": {[]string{"hostname1"}, true},
				"ip2": {[]string{"hostname2"}, false},
			},
			valkeyFQDNToIDMap:     `{"hostname1": "id1", "hostname2": "id2"}`,
			valkeyClusterConfPath: "test_cluster.conf",
			confContent: `
56cac18e538888e2fb81b09b8491e819d2bda1e1 ip1:6379@16379 master,nofailover - 0 1747228909000 44 connected 2731-5460 10923-13653
d36dacb40728f82b6453a611941cded23915d24a ip2:6379@16379 master,nofailover - 0 1747228909000 44 connected 5461-10922 [10923->3d68e5b49b010564b64c8a4ac26536a8d6a756f8]
`,
			expectedErr: NewMigratingSlotsError("5461-10922 [10923->3d68e5b49b010564b64c8a4ac26536a8d6a756f8]"),
		},
		{
			name: "[success] failed line is filtered out",
			ipAddrs: map[string]Addrs{
				// "ip1": {[]string{"hostname1"}, true},
				"ip2": {[]string{"hostname2"}, true},
			},
			valkeyFQDNToIDMap:     `{"hostname1": "id1", "hostname2": "id2"}`,
			valkeyClusterConfPath: "test_cluster.conf",
			confContent: `
17b6be48fa511f0adad8c887dc01dd7067e7bfe5 ip1:6379@16379,hostname1,tls-port=0,shard-id=078c4272db66981a314129680c33a980ebd2e037 master,fail,nofailover - 1750694758775 1750694758775 419 connected
d36dacb40728f82b6453a611941cded23915d24a ip2:6379@16379,,tls-port=0,shard-id=3e0c8579c9f33534b4ccaafe168eb9a1d97c116e master,fail,nofailover - 1750771752642 1750771748000 53 connected
56cac18e538888e2fb81b09b8491e819d2bda1e1 ip2:6379@16379 master,nofailover - 0 1747228909000 44 connected 2731-5460 10923-13653
`,
			expected: map[string][][]string{
				"id1": {},
				"id2": {{"2731", "5460"}, {"10923", "13653"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockNet := mocks.NewMockNetI(mockCtrl)
			defer mockCtrl.Finish()

			// Mock LookupAddr
			for ip, hostnames := range tt.ipAddrs {
				if hostnames.lookedup {
					mockNet.EXPECT().LookupAddr(ip).Return(hostnames.addrs, nil)
				}
			}

			// Mock viper configuration
			viper.Set(conf.RedisFQDNToIDMap, tt.valkeyFQDNToIDMap)
			viper.Set(conf.RedisClusterConfPath, tt.valkeyClusterConfPath)

			// Create a test file with cluster info
			require.NoError(t, os.WriteFile(tt.valkeyClusterConfPath, []byte(tt.confContent), 0644))
			defer os.Remove(tt.valkeyClusterConfPath)

			slotsMap, err := GetSlotsMap(mockNet)
			if tt.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorAs(t, err, &tt.expectedErr)
				require.Equal(t, tt.expectedErr.Error(), err.Error())
			}

			if len(slotsMap) != len(tt.expected) {
				t.Errorf("expected %d entries, got %d", len(tt.expected), len(slotsMap))
			}

			for id, slots := range tt.expected {
				if len(slotsMap[id]) != len(slots) {
					t.Errorf("expected %d slots for ID %s, got %d", len(slots), id, len(slotsMap[id]))
				}
			}
		})
	}
}

func TestValidateFqdns(t *testing.T) {
	tests := []struct {
		name          string
		fqdnToIDMap   map[string]string
		idToSlots     map[string][][]string
		expectedError string
	}{
		{
			name: "[success] simple",
			fqdnToIDMap: map[string]string{
				"host1": "id1",
				"host2": "id2",
			},
			idToSlots: map[string][][]string{
				"id1": {{"0", "10000"}},
				"id2": {{"10001", "16384"}},
			},
			expectedError: "",
		},
		{
			name:        "[success] complex",
			fqdnToIDMap: map[string]string{"host1": "id4", "host2": "id2", "host3": "id3", "host4": "id1", "host5": "id3", "host6": "id2", "host7": "id4", "host8": "id1", "host9": "id1", "host10": "id4", "host11": "id2", "host12": "id3"},
			idToSlots: map[string][][]string{
				"id1": {{"0", "5460"}},
				"id2": {{"5461", "10922"}},
				"id3": {{"10923", "16383"}},
				"id4": {{}},
			},
			expectedError: "",
		},
		{
			name: "[fail] missing ID case",
			fqdnToIDMap: map[string]string{
				"host1": "id1",
				"host2": "id2",
			},
			idToSlots: map[string][][]string{
				"id1": {{"0", "10000"}},
			},
			expectedError: "failed to find all IDs from map[host1:id1 host2:id2]\nfound only map[id1:[[0 10000]]]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateFqdns(tt.fqdnToIDMap, tt.idToSlots)
			if tt.expectedError == "" {
				if err != nil {
					t.Errorf("expected no error, but got: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected an error, but got nil")
				} else if err.Error() != tt.expectedError {
					t.Errorf("error message mismatch. Expected: %s, Got: %s", tt.expectedError, err.Error())
				}
			}
		})
	}
}

func TestValidateFqdns_MissingID(t *testing.T) {
	fqdnToIDMap := map[string]string{
		"host1": "id1",
		"host2": "id2",
	}

	idToSlots := map[string][][]string{
		"id1": {{"0", "10000"}},
	}

	_, err := validateFqdns(fqdnToIDMap, idToSlots)
	if err == nil {
		t.Errorf("expected an error for missing ID 'id2'")
		return
	}

	expectedErrorMessage := "failed to find all IDs from map[host1:id1 host2:id2]\nfound only map[id1:[[0 10000]]]"
	if err.Error() != expectedErrorMessage {
		t.Errorf("error message mismatch. Expected: %s, Got: %s", expectedErrorMessage, err.Error())
	}
}
