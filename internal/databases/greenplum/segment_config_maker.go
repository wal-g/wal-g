package greenplum

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/wal-g/utility"
)

type SegConfigMaker interface {
	Make(SegmentMetadata) (cluster.SegConfig, error)
}

type RestoreCfgSegMaker struct {
	restoreCfg ClusterRestoreConfig
}

func NewRestoreCfgSegMaker(restoreConfigReader io.Reader) (SegConfigMaker, error) {
	restoreCfgBytes, err := io.ReadAll(restoreConfigReader)
	if err != nil {
		return nil, err
	}

	var restoreCfg ClusterRestoreConfig
	err = json.Unmarshal(restoreCfgBytes, &restoreCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal the provided restore config: %v", err)
	}

	return &RestoreCfgSegMaker{restoreCfg}, nil
}

func (c *RestoreCfgSegMaker) Make(metadata SegmentMetadata) (cluster.SegConfig, error) {
	segmentCfg := metadata.ToSegConfig()
	segRestoreCfg, ok := c.restoreCfg.Segments[metadata.ContentID]
	if !ok {
		return cluster.SegConfig{},
			fmt.Errorf(
				"could not find content ID %d in the provided restore configuration",
				metadata.ContentID)
	}
	segmentCfg.Hostname = segRestoreCfg.Hostname
	segmentCfg.Port = segRestoreCfg.Port
	segmentCfg.DataDir = segRestoreCfg.DataDir
	return segmentCfg, nil
}

type InPlaceSegMaker struct{}

func (c *InPlaceSegMaker) Make(metadata SegmentMetadata) (cluster.SegConfig, error) {
	return metadata.ToSegConfig(), nil
}

func NewSegConfigMaker(restoreCfgPath string, inPlaceRestore bool) (SegConfigMaker, error) {
	if inPlaceRestore {
		return &InPlaceSegMaker{}, nil
	}

	file, err := os.Open(restoreCfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open the provided restore config file: %v", err)
	}
	defer utility.LoggedClose(file, "")

	return NewRestoreCfgSegMaker(file)
}
