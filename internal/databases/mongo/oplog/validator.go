package oplog

import "fmt"

type ErrorCode int

const (
	ErrorMessagePrefix           = "oplog validate error"
	SplitFound         ErrorCode = iota
	VersionChanged     ErrorCode = iota
	CollectionRenamed  ErrorCode = iota
)

var ErrorDescriptions = map[ErrorCode]string{
	SplitFound:        "last known document was not found",
	VersionChanged:    "schema version of the user credential documents changed",
	CollectionRenamed: "collection renamed",
}

type Error struct {
	code ErrorCode
	msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s - %s", ErrorMessagePrefix, ErrorDescriptions[e.code], e.msg)
}

func NewError(code ErrorCode, msg string) error {
	return &Error{code, msg}
}

type Validator interface {
	ValidateRecord(op Record) error
}

type ValidateFunc func(op Record) error

func (f ValidateFunc) ValidateRecord(op Record) error {
	return f(op)
}

// ValidateSplittingOps returns error if oplog record breaks archive replay possibility.
// TODO: unit tests
func ValidateSplittingOps(op Record) error {
	if op.NS == "admin.system.version" {
		return NewError(VersionChanged, fmt.Sprintf("operation '%s'", op.OP))
	}
	if op.OP == "renameCollections" {
		return NewError(CollectionRenamed, op.NS)
	}
	return nil
}
