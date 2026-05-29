package stats

import (
	"context"
	"fmt"
	"slices"
)

var _ Collector = &nopCollector{}

type nopCollector struct {
	storagesInOrder []string
}

// NewNopCollector creates a Collector that does nothing, just returns storages as if they were always alive.
func NewNopCollector(storagesInOrder []string) Collector {
	return &nopCollector{storagesInOrder: storagesInOrder}
}

func (nc *nopCollector) AllAliveStorages(_ context.Context) ([]string, error) {
	return nc.storagesInOrder, nil
}

func (nc *nopCollector) FirstAliveStorage(_ context.Context) (*string, error) {
	return &nc.storagesInOrder[0], nil
}

func (nc *nopCollector) SpecificStorage(_ context.Context, name string) (bool, error) {
	if slices.Contains(nc.storagesInOrder, name) {
		return true, nil
	}
	return false, fmt.Errorf("unknown storage %q", name)
}

func (nc *nopCollector) ReportOperationResult(_ string, _ OperationWeight, _ bool) {
	// Nothing to report
}

func (nc *nopCollector) Close() error {
	// Nothing to close
	return nil
}
