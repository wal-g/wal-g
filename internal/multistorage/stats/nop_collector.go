package stats

import "fmt"

var _ Collector = &nopCollector{}

type nopCollector struct {
	storagesInOrder []string
}

// NewNopCollector creates a Collector that does nothing, just returns storages as if they were always alive.
func NewNopCollector(storagesInOrder []string) Collector {
	return &nopCollector{storagesInOrder: storagesInOrder}
}

func (nc *nopCollector) AllAliveStorages() ([]string, error) {
	return nc.storagesInOrder, nil
}

func (nc *nopCollector) FirstAliveStorage() (*string, error) {
	return &nc.storagesInOrder[0], nil
}

func (nc *nopCollector) SpecificStorage(name string) (bool, error) {
	for _, s := range nc.storagesInOrder {
		if s == name {
			return true, nil
		}
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
