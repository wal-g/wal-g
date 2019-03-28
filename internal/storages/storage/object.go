package storage

import (
	"time"
)

type Object interface {
	GetName() string
	GetLastModified() time.Time
}

type SortableObjectsSlice []Object

var less func(object1 Object, object2 Object) bool

func (slice SortableObjectsSlice) Len() int {
	return len(slice)
}

func (slice SortableObjectsSlice) Less(i, j int) bool {
	return less(slice[i], slice[j])
}

func (slice SortableObjectsSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func SetLessFunction(newLess func(object1 Object, object2 Object) bool) {
	less = newLess
}
