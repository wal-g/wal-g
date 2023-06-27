package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/wal-g/wal-g/internal/daemon"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	cmdUsageMessageTemplate = `Error: {{.Error}}

walg-daemon-client is lightweight client for WAL-G daemon

Usage:
  walg-daemon-client socket {{if .CommandUsage}}{{.CommandUsage}}{{else}}command [command_args]{{end}} [flags]
{{if not .CommandUsage}}
Arguments:
  socket	- name of unix socket to communicate with wal-g daemon
  command	- command to send to the daemon: wal-push, wal-fetch
  command_args	- command specific arguments
{{end}}
Flags:
`
)

type commandOpts struct {
	name    string
	msgType daemon.SocketMessageType
	args    []string

	options *daemon.RunOptions
}

type templateData struct {
	Error        error
	CommandUsage string
}

var (
	// These variables are here only to show current version. They are set in makefile during build process
	version     = "devel"
	gitRevision = "devel"
	buildDate   = "devel"

	errCommandArguments = fmt.Errorf("not enough command arguments")

	commands = map[string]*commandOpts{
		"wal-push": {
			msgType: daemon.WalPushType,
			args:    []string{"wal_filepath"},
		},
		"wal-fetch": {
			msgType: daemon.WalFetchType,
			args:    []string{"wal_name", "destination_filename"},
		},
	}
)

func parseArgs(args []string) (*commandOpts, *flag.FlagSet, error) {
	opts := &daemon.RunOptions{}
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.DurationVar(&opts.DaemonOperationTimeout, "timeout", 60*time.Second, "daemon operation execution timeout")
	fs.DurationVar(&opts.DaemonSocketConnectionTimeout, "connection-timeout", 5*time.Second, "daemon socket connection timeout")

	if len(args) < 2 {
		return nil, fs, fmt.Errorf("not enough arguments")
	}
	opts.SocketName = args[0]

	command := strings.ToLower(args[1])
	cmd, ok := commands[command]
	if !ok {
		return nil, fs, fmt.Errorf("unsupported command %v", command)
	}

	cmd.name = command
	if len(args) < 2+len(cmd.args) {
		return cmd, fs, errCommandArguments
	}
	opts.MessageArgs = args[2 : 2+len(cmd.args)]
	opts.MessageType = cmd.msgType

	if len(args) > 2+len(cmd.args) {
		err := fs.Parse(args[2+len(cmd.args):])
		if err != nil {
			return nil, fs, err
		}
	}

	cmd.options = opts
	return cmd, fs, nil
}

func main() {
	usageTemplate, err := template.New("usage").Parse(cmdUsageMessageTemplate)
	if err != nil {
		log.Fatal("can't parse command usage template")
	}

	if len(os.Args) > 1 && strings.ToLower(os.Args[1]) == "--version" {
		fmt.Println(strings.Join([]string{version, gitRevision, buildDate}, "\t"))
		return
	}

	cmd, fs, err := parseArgs(os.Args[1:])
	if err != nil {
		tmplOpts := templateData{
			Error: err,
		}
		if err == errCommandArguments {
			tmplOpts.CommandUsage = fmt.Sprintf("%v %v", cmd.name, strings.Join(cmd.args, " "))
		}
		_ = usageTemplate.Execute(os.Stdout, tmplOpts)
		fs.PrintDefaults()
		os.Exit(1)
	}
	if _, err = os.Stat(cmd.options.SocketName); err != nil {
		log.Fatalf("daemon socket '%v' doesn't exist or is unavailable:\n\t%v", cmd.options.SocketName, err)
	}

	err, response := daemon.SendCommand(cmd.options)
	if err != nil {
		if response == daemon.ArchiveNonExistenceType {
			fmt.Println(err.Error())
			os.Exit(postgres.ExIoError)
		}
		log.Fatal(err)
	}
}
