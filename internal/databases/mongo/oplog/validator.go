package oplog

import (
	"context"
	"fmt"
	"sync"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

// Validator defines interface to verify given oplog records.
type Validator interface {
	Validate(context.Context, chan models.Oplog, *sync.WaitGroup) (chan models.Oplog, chan error, error)
}

// DBValidator implements validation for database source.
type DBValidator struct {
	since models.Timestamp
}

// NewDBValidator builds DBValidator.
func NewDBValidator(since models.Timestamp) *DBValidator {
	return &DBValidator{since}
}

// Validate verifies incoming records.
func (dbv *DBValidator) Validate(ctx context.Context, in chan models.Oplog, wg *sync.WaitGroup) (out chan models.Oplog, errc chan error, err error) {
	checkFirstTS := true
	zeroTS := models.Timestamp{}
	if dbv.since == zeroTS {
		checkFirstTS = false
	}

	out = make(chan models.Oplog)
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
					errc <- models.NewError(models.SplitFound, fmt.Sprintf("expected first ts is %v, but %v is given", dbv.since, op.TS))
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
func ValidateSplittingOps(op models.Oplog) error {
	//if op.NS == "admin.system.version" {
	//	return models.NewError(models.VersionChanged, fmt.Sprintf("operation '%s'", op.OP))
	//}
	if op.OP == "renameCollections" {
		return models.NewError(models.CollectionRenamed, op.NS)
	}
	return nil
}
