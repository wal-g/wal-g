package internal

import "time"

type GSStorageObject struct {
	updated time.Time
	name    string
}

func (object *GSStorageObject) GetName() string {
	return object.name
}

func (object *GSStorageObject) GetLastModified() time.Time {
	return object.updated
}
