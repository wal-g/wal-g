package internal

import "time"

type AzureStorageObject struct {
	updated time.Time
	name    string
}

func (object *AzureStorageObject) GetName() string {
	return object.name
}

func (object *AzureStorageObject) GetLastModified() time.Time {
	return object.updated
}
