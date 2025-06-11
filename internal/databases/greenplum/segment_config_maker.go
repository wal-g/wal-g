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

func NewRestoreCfgSegMaker(restoreCfg ClusterRestoreConfig) SegConfigMaker {
	return &RestoreCfgSegMaker{restoreCfg}
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

	restoreCfg, err := readRestoreConfig(restoreCfgPath)
	if err != nil {
		return nil, err
	}

	return NewRestoreCfgSegMaker(restoreCfg), nil
}

func readRestoreConfig(path string) (ClusterRestoreConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return ClusterRestoreConfig{}, fmt.Errorf("failed to open the provided restore config file: %v", err)
	}
	defer utility.LoggedClose(file, "")

	restoreCfgBytes, err := io.ReadAll(file)
	if err != nil {
		return ClusterRestoreConfig{}, err
	}

	var restoreCfg ClusterRestoreConfig
	err = json.Unmarshal(restoreCfgBytes, &restoreCfg)
	if err != nil {
		return ClusterRestoreConfig{}, fmt.Errorf("failed to unmarshal the provided restore config: %v", err)
	}

	return restoreCfg, nil
}
