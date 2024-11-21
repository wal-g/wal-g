package internal

import (
	"sync"
)

type TarFileSets interface {
	AddFile(name string, file string)
	AddFiles(name string, files []string)
	Get() map[string][]string
}

type RegularTarFileSets struct {
	data  map[string][]string
	mutex sync.RWMutex
}

func NewRegularTarFileSets() *RegularTarFileSets {
	return &RegularTarFileSets{
		data: make(map[string][]string),
	}
}

func (tarFileSets *RegularTarFileSets) AddFile(name string, file string) {
	tarFileSets.mutex.Lock()
	defer tarFileSets.mutex.Unlock()

	tarFileSets.data[name] = append(tarFileSets.data[name], file)
}

func (tarFileSets *RegularTarFileSets) AddFiles(name string, files []string) {
	tarFileSets.mutex.Lock()
	defer tarFileSets.mutex.Unlock()

	tarFileSets.data[name] = append(tarFileSets.data[name], files...)
}

func (tarFileSets *RegularTarFileSets) Get() map[string][]string {
	tarFileSets.mutex.RLock()
	defer tarFileSets.mutex.RUnlock()

	// Create a copy to ensure thread safety
	result := make(map[string][]string, len(tarFileSets.data))
	for k, v := range tarFileSets.data {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		result[k] = newSlice
	}
	return result
}

type NopTarFileSets struct {
}

func NewNopTarFileSets() *NopTarFileSets {
	return &NopTarFileSets{}
}

func (tarFileSets *NopTarFileSets) AddFile(name string, file string) {
}

func (tarFileSets *NopTarFileSets) AddFiles(name string, files []string) {
}

func (tarFileSets *NopTarFileSets) Get() map[string][]string {
	return make(map[string][]string)
}
