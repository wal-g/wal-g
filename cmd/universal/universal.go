package universal

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/cmd/etcd"
	"github.com/wal-g/wal-g/cmd/fdb"
	"github.com/wal-g/wal-g/cmd/mongo"
	"github.com/wal-g/wal-g/cmd/mysql"
	"github.com/wal-g/wal-g/cmd/pg"
	"github.com/wal-g/wal-g/cmd/redis"
	"github.com/wal-g/wal-g/cmd/sqlserver"
)

var (
	universalCmd = &cobra.Command{
		Use:   "wal-g",
		Short: "Universal database backup tool",
	}
)

func Execute() {
	if err := universalCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	etcdCmd := etcd.GetCmd()
	etcdCmd.Use = "etcd"
	universalCmd.AddCommand(etcdCmd)

	fdbCmd := fdb.GetCmd()
	fdbCmd.Use = "fdb"
	universalCmd.AddCommand(fdbCmd)

	mongoCmd := mongo.GetCmd()
	mongoCmd.Use = "mongo"
	universalCmd.AddCommand(mongoCmd)

	mysqlCmd := mysql.GetCmd()
	mysqlCmd.Use = "mysql"
	universalCmd.AddCommand(mysqlCmd)

	pgCmd := pg.GetCmd()
	pgCmd.Use = "pg"
	universalCmd.AddCommand(pgCmd)

	redisCmd := redis.GetCmd()
	redisCmd.Use = "redis"
	universalCmd.AddCommand(redisCmd)

	sqlserverCmd := sqlserver.GetCmd()
	sqlserverCmd.Use = "sqlserver"
	universalCmd.AddCommand(sqlserverCmd)
}
