package walg

import "testing"

func TestDeleteArgsParsingRetain(t *testing.T) {
	var args DeleteCommandArguments
	command := []string{"delete", "retain", "5"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}

	if !args.retain || args.find_full || args.target != "5" {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "retain", "FULL", "5"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.retain || !args.full || args.target != "5" {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "retain", "FIND_FULL", "5"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.retain || args.full || args.target != "5" || !args.find_full {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "re123tain", "FULL", "5"}

	if !parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand parsed wrong input")
	}
}

func TestDeleteArgsParsingBefore(t *testing.T) {
	var args DeleteCommandArguments
	command := []string{"delete", "before", "x"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}

	if !args.before || args.find_full || args.target != "x" || args.retain {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "before", "FIND_FULL", "x"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.before || !args.find_full || args.target != "x" || args.beforeTime != nil {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete", "before", "FIND_FULL", "2014-11-12T11:45:26.371Z"}

	if parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand failed")
	}
	if !args.before || !args.find_full || args.beforeTime == nil {
		t.Fatal("Parsing was wrong")
	}

	command = []string{"delete"}

	if !parseAndTestFail(command, &args) {
		t.Fatal("Parsing of delete comand parsed wrong input")
	}
}

func parseAndTestFail(command []string, arguments *DeleteCommandArguments) bool {
	var failed bool
	result := ParseDeleteArguments(command, func() { failed = true })
	*arguments = result
	return failed
}
