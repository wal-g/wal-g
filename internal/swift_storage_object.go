package internal

import "time"

type SwiftStorageObject struct {
	updated time.Time
	name    string
}

func (object *SwiftStorageObject) GetName() string {
	return object.name
}

func (object *SwiftStorageObject) GetLastModified() time.Time {
	return object.updated
}
