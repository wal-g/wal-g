package models

import "fmt"

// ErrorCode ...
type ErrorCode int

// Error codes generation
const (
	ValidationMessagePrefix           = "oplog validate error"
	SplitFound              ErrorCode = iota
	VersionChanged          ErrorCode = iota
	CollectionRenamed       ErrorCode = iota
)

// ErrorDescriptions maps error codes to messages
var ErrorDescriptions = map[ErrorCode]string{
	SplitFound:        "last known document was not found",
	VersionChanged:    "schema version of the user credential documents changed",
	CollectionRenamed: "collection renamed",
}

// Error ...
type Error struct {
	code ErrorCode
	msg  string
}

// Error ...
func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s - %s", ValidationMessagePrefix, ErrorDescriptions[e.code], e.msg)
}

// NewError builds Error with error code and message
func NewError(code ErrorCode, msg string) error {
	return &Error{code, msg}
}
