package internal

import (
	"encoding/json"
	"io"
)

type OpsStat struct {
	Success map[string]int `json:"succeeded"`
	Fail    map[string]int `json:"failed"`
	Docs    map[string]int `json:"docs"`
}

type LoadStat struct {
	CmdStat OpsStat `json:"commands"`
	TxnStat OpsStat `json:"transactions"`
}

func NewLoadStat() *LoadStat {
	return &LoadStat{
		CmdStat: OpsStat{
			Success: make(map[string]int),
			Fail:    make(map[string]int),
			Docs:    make(map[string]int),
		},
		TxnStat: OpsStat{
			Success: make(map[string]int),
			Fail:    make(map[string]int),
			Docs:    make(map[string]int),
		},
	}
}

func updateStatWithMongoOpLog(stat *OpsStat, log OpInfo) {
	if log.status {
		stat.Success[log.opName]++
	} else {
		stat.Fail[log.opName]++
		return
	}
	var docs int
	if log.res != nil && log.res["n"] != nil {
		docs = int(log.res["n"].(int32))
	}
	stat.Docs[log.opName] += docs
}

func CollectStat(opInfoCh <-chan OpInfo) LoadStat {
	stat := NewLoadStat()
	for opInfo := range opInfoCh {
		updateStatWithMongoOpLog(&stat.CmdStat, opInfo)
		if opInfo.opName == "transaction" {
			for _, cmdLog := range opInfo.subcmds {
				updateStatWithMongoOpLog(&stat.TxnStat, cmdLog)
			}
		}
	}
	return *stat
}

func PrintStat(stat LoadStat, writer io.Writer) error {
	bstat, err := json.MarshalIndent(stat, "", "    ")
	if err != nil {
		return err
	}
	_, err = writer.Write(bstat)
	return err
}
