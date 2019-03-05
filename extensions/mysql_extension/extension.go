package main

import (
	"fmt"
	"github.com/wal-g/wal-g/extensions/mysql_extension/mysql"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
)

type extension struct {
}

var allowedConfigKeys = map[string]*string{
	"WALG_MYSQL_DATASOURCE_NAME": nil,
	"WALG_MYSQL_BINLOG_DST":      nil,
	"WALG_MYSQL_BINLOG_END_TS":   nil,
}

func (*extension) TryPrintHelp(command string, args []string) bool {
	firstArgument := ""
	if len(args) > 1 {
		firstArgument = args[1]
	}
	if firstArgument == "-h" || firstArgument == "--help" || (firstArgument == "" && command != "stream-fetch"&& command != "stream-push" ) {
		switch command {
		case "mysql-cron":
			fmt.Printf("usage:\twal-g mysql-cron /path/to/backup\n\n")
		case "stream-fetch":
			fmt.Printf("usage:\twal-g stream-fetch backup-name\n\n")
		case "stream-push":
			fmt.Printf("usage:\twal-g stream-push backup-name\n\n")
		}
		return true
	}
	return false
}

func (*extension) HasCommand(command string) bool {
	return command == "mysql-cron" || command == "stream-push" || command == "stream-fetch"
}

func (*extension) GetAllowedConfigKeys() map[string]*string {
	return allowedConfigKeys
}

func (*extension) Execute(command string, uploader *internal.Uploader, folder storage.Folder, args []string) {
	firstArgument := ""
	if len(args) > 1 {
		firstArgument = args[1]
	}
	switch command {
	case "mysql-cron":
		mysql.HandleMySQLCron(&mysql.Uploader{Uploader: uploader}, firstArgument)
	case "stream-fetch":
		mysql.HandleStreamFetch(firstArgument, folder)
	case "stream-push":
		mysql.HandleStreamPush(&mysql.Uploader{Uploader: uploader}, firstArgument)
	}
}

func (*extension) Flush(time internal.BackupTime, folder storage.Folder) {
	mysql.DeleteOldBinlogs(time, folder)
}

var Extension extension