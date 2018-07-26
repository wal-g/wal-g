package walg_test

import (
	"github.com/wal-g/wal-g"
	"testing"
)

func TestDeleteArgsParsingRetain(t *testing.T) {
	var args walg.DeleteCommandArguments
	command := []string{"delete", "retain", "5"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}

	if !args.Retain || args.FindFull || args.Target != "5" {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "retain", "FULL", "5"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.Retain || !args.Full || args.Target != "5" {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "retain", "FIND_FULL", "5"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.Retain || args.Full || args.Target != "5" || !args.FindFull {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "re123tain", "FULL", "5"}

	if !parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand parsed wrong input")
	}
}

func TestDeleteArgsParsingBefore(t *testing.T) {
	var args walg.DeleteCommandArguments
	command := []string{"delete", "before", "x"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}

	if !args.Before || args.FindFull || args.Target != "x" || args.Retain {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "before", "FIND_FULL", "x"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.Before || !args.FindFull || args.Target != "x" || args.BeforeTime != nil {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "before", "FIND_FULL", "2014-11-12T11:45:26.371Z"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.Before || !args.FindFull || args.BeforeTime == nil {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete"}

	if !parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand parsed wrong input")
	}
}

func parseAndTestFail(command []string, arguments *walg.DeleteCommandArguments) bool {
	var failed bool
	result := walg.ParseDeleteArguments(command, func() { failed = true })
	*arguments = result
	return failed
}
