package models

import (
	"fmt"
	"strings"
)

type OpsStat struct {
	Success map[string]int     `json:"succeeded"`
	Fail    map[string]int     `json:"failed"`
	Docs    map[string]int     `json:"docs"`
	Errors  map[string][]error `json:"errors"`
}

type LoadStat struct {
	CmdStat OpsStat `json:"commands"`
	TxnStat OpsStat `json:"transactions"`
}

func (ls *LoadStat) GetError() error {
	var errs []string
	cmdFailed := len(ls.CmdStat.Fail)
	txnFailed := len(ls.TxnStat.Fail)

	if cmdFailed > 0 {
		errs = append(errs, fmt.Sprintf("%d commands failed", cmdFailed))
	}
	if txnFailed > 0 {
		errs = append(errs, fmt.Sprintf("%d transactions failed", txnFailed))
	}
	if len(errs) > 0 {
		return fmt.Errorf("error occurred during load: %s", strings.Join(errs, ", "))
	}

	return nil
}

func NewLoadStat() *LoadStat {
	return &LoadStat{
		CmdStat: OpsStat{
			Success: make(map[string]int),
			Fail:    make(map[string]int),
			Errors:  make(map[string][]error),
			Docs:    make(map[string]int),
		},
		TxnStat: OpsStat{
			Success: make(map[string]int),
			Fail:    make(map[string]int),
			Errors:  make(map[string][]error),
			Docs:    make(map[string]int),
		},
	}
}
