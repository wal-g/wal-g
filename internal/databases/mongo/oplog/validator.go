package oplog

import (
	"context"
	"fmt"
	"sync"
)

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

// Validator defines interface to verify given oplog records.
type Validator interface {
	Validate(context.Context, chan Record, *sync.WaitGroup) (chan Record, chan error, error)
}

// DBValidator implements validation for database source
type DBValidator struct {
	since Timestamp
}

// NewDBValidator builds DBValidator.
// TODO: switch to functional args
func NewDBValidator(since Timestamp) *DBValidator {
	return &DBValidator{since}
}

// Validate verifies incoming records.
func (dbv *DBValidator) Validate(ctx context.Context, in chan Record, wg *sync.WaitGroup) (out chan Record, errc chan error, err error) {
	checkFirstTS := true
	zeroTS := Timestamp{}
	if dbv.since == zeroTS {
		checkFirstTS = false
	}

	out = make(chan Record)
	errc = make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errc)
		defer close(out)

		for op := range in {
			if checkFirstTS {
				if op.TS != dbv.since {
					// TODO: handle gap
					errc <- NewError(SplitFound, fmt.Sprintf("expected first ts is %v, but %v is given", dbv.since, op.TS))
					return
				}
				checkFirstTS = false
			}
			if err := ValidateSplittingOps(op); err != nil {
				errc <- err
				return
			}
			select {
			case out <- op:
			case <-ctx.Done():
				return
			}

		}
	}()

	return out, errc, nil
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
