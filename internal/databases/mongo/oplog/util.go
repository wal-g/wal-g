package oplog

import (
	"fmt"
	"slices"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// filterUUIDs recursively removes 'ui' entries from ops, including nested applyOps ops.
func filterUUIDs(op *db.Oplog) (*db.Oplog, error) {
	// Remove UUIDs from oplog entries
	op.UI = nil

	// Check for and filter nested applyOps ops
	if op.Operation == "c" && isApplyOpsCmd(op.Object) {
		filtered, err := newFilteredApplyOps(&op.Object)
		if err != nil {
			ret := db.Oplog{}
			return &ret, err
		}
		op.Object = filtered
	}

	return op, nil
}

// newFilteredApplyOps iterates over nested ops in an applyOps document and
// returns a new applyOps document that omits the 'ui' field from nested ops.
func newFilteredApplyOps(cmd *bson.D) (bson.D, error) {
	ops, err := unwrapNestedApplyOps(cmd)
	if err != nil {
		return nil, err
	}

	for i := range ops {
		_, err = filterUUIDs(&ops[i])
		if err != nil {
			return nil, err
		}
	}

	doc, err := wrapNestedApplyOps(ops)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func filterNestedApplyOps(op *db.Oplog, catchUp bool) (allFiltered bool, err error) {
	if op.Operation != "c" || !isApplyOpsCmd(op.Object) {
		return false, nil
	}

	ops, err := unwrapNestedApplyOps(&op.Object)
	if err != nil {
		return false, err
	}

	kept := make([]db.Oplog, 0, len(ops))
	hasNestedApplyOps := false
	for i := range ops {
		if shouldSkip(&ops[i], catchUp) != nil {
			continue
		}
		if ops[i].Operation == "c" && isApplyOpsCmd(ops[i].Object) {
			hasNestedApplyOps = true
		}
		nestedFiltered, err := filterNestedApplyOps(&ops[i], catchUp)
		if err != nil {
			return false, err
		}
		if !nestedFiltered {
			kept = append(kept, ops[i])
		}
	}

	if len(kept) == 0 {
		return true, nil
	}
	if len(kept) == len(ops) && !hasNestedApplyOps {
		return false, nil
	}

	op.Object, err = replaceNestedApplyOps(op.Object, kept)
	return false, err
}

func replaceNestedApplyOps(cmd bson.D, ops []db.Oplog) (bson.D, error) {
	wrapped, err := wrapNestedApplyOps(ops)
	if err != nil {
		return nil, err
	}

	result := slices.Clone(cmd)
	for i := range result {
		if result[i].Key == "applyOps" {
			result[i].Value = wrapped[0].Value
		}
	}
	return result, nil
}

// isApplyOpsCmd returns true if a document seems to be an applyOps command.
func isApplyOpsCmd(cmd bson.D) bool {
	return slices.ContainsFunc(cmd, func(v bson.E) bool { return v.Key == "applyOps" })
}

type nestedApplyOps struct {
	ApplyOps []db.Oplog `bson:"applyOps"`
}

// unwrapNestedApplyOps converts a bson.D to a typed data structure.
func unwrapNestedApplyOps(doc *bson.D) ([]db.Oplog, error) {
	// Doc to bytes
	bs, err := bson.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf("remarshal nested applyOps: %w", err)
	}

	// Bytes to typed data
	var cmd nestedApplyOps
	err = bson.Unmarshal(bs, &cmd)
	if err != nil {
		return nil, fmt.Errorf("unwrap nested applyOps: %w", err)
	}

	return cmd.ApplyOps, nil
}

// wrapNestedApplyOps converts a typed data structure to a bson.D.
func wrapNestedApplyOps(ops []db.Oplog) (bson.D, error) {
	cmd := &nestedApplyOps{ApplyOps: ops}

	// Typed data to bytes
	raw, err := bson.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("rewrap nested applyOps op: %w", err)
	}

	// Bytes to doc
	var doc bson.D
	err = bson.Unmarshal(raw, &doc)
	if err != nil {
		return nil, fmt.Errorf("reunmarshal nested applyOps op: %w", err)
	}

	return doc, nil
}
