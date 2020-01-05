package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func parseGTIDFromFile() (string, error) {
	file, err := os.Open("/var/lib/mysql/xtrabackup_binlog_info")
	defer file.Close()

	if err != nil {
		log.Fatalf("failed opening file: %s", err)
		return "", nil
	}
	byt, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	arr := strings.Fields(string(byt))
	return arr[len(arr)-1], nil
}

func setGTIDFromSnapshot() error {
	datasourceName := "sbtest:@(localhost:3306)/mysql"
	db, err := sqlx.Connect("mysql", datasourceName)
	if err != nil {
		return err
	}

	if rows, err := db.Queryx("SELECT @@GLOBAL.GTID_EXECUTED"); err != nil {
		return err
	} else {
		if ans, err := rows.Columns(); err != nil {
			return err
		} else {
			fmt.Print(strings.Join(ans, " "))
		}
	}

	snapshotGTIDExecuted, err := parseGTIDFromFile()
	sqls := []string{
		"RESET MASTER",
		fmt.Sprintf("SET @@GLOBAL.GTID_PURGED='%s'", snapshotGTIDExecuted),
	}

	for _, sql := range sqls {
		_, err = db.Queryx(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := setGTIDFromSnapshot(); err != nil {
		panic(err)
	}
}
