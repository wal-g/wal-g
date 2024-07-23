# When MySQL Meets Time Travel: Adventures in Point-in-Time Recovery

## Intro

Point in Time Recovery (PiTR) - an approach to restore database to exact moment in time (with millisecond or exact-transaction precision).
It is very useful in disaster recovery scenarios: it reduces amount of data you may lose.

![IntroIllustration](resources/mysql_pitr_01.jpg)

In order to restore database to any point in time we should have database snapshot (backup) and apply all changes (binlogs) - that is how Replicated State Machine works. If all changes are deterministic - resulting database will not differ from original one.

### Stand

In this manual we will use following stand:
 * percona-server 8.0.x
 * wal-g 3.0+


mysqld config:
```
# Set unique server_id
server_id=1

# Enable GTIDs
gtid_mode=ON
enforce_gtid_consistency=ON

# Enable Group Commit
slave_parallel_type=LOGICAL_CLOCK
slave_parallel_workers=8
```

`wal-g` config (in `walg.properties` format):
```
# Typical MySQL config
WALG_MYSQL_DATASOURCE_NAME=root:Qwerty12345@tcp(localhost:3306)/test
WALG_STREAM_CREATE_COMMAND=/usr/bin/xtrabackup --backup --stream=xbstream --datadir=/var/lib/mysql --user=root --password=Qwerty12345
WALG_STREAM_RESTORE_COMMAND=xbstream -x -C /var/lib/mysql
WALG_MYSQL_BACKUP_PREPARE_COMMAND=xtrabackup --prepare --target-dir=/var/lib/mysql
WALG_MYSQL_BINLOG_REPLAY_COMMAND=mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql
WALG_MYSQL_BINLOG_DST=/var/lib/mysql/my-binlogs

# (De)compress backup in 6 threads:
WALG_STREAM_SPLITTER_PARTITIONS=6
WALG_COMPRESSION_METHOD=zstd

# Storage
WALG_S3_PREFIX=s3://backet-name/mysql/
AWS_ENDPOINT=storage.yandexcloud.net
AWS_ACCESS_KEY_ID=<key_id>
AWS_SECRET_ACCESS_KEY=<access_key>
```

In order to create backup we should run:
```bash
wal-g-mysql --turbo --config .walg.properties backup-push
```

In order to store binlogs we should regularly run:
```bash
wal-g-mysql --config .walg.properties  binlog-push
```

## Classic PiTR recovery

![IntroIllustration](resources/mysql_pitr_02.jpg)

Official documentation [suggests](https://dev.mysql.com/doc/refman/8.0/en/point-in-time-recovery.html) to use 'classic' PiTR. That is basically as simple as  `mysqlbinlog binlog_files | mysql -u root -p`.

This approach is well-tested and works great in most cases. However, you may notice that it suboptimal: we convert binlogs to palin SQL, send it to MySQL and it parses SQL, and executes it. It consumes quite a lot of CPU. Moreover, during this process information about group commits are lost and MySQL cannot apply transactions concurrently.


### With `wal-g` it will look like:

![IntroIllustration](resources/mysql_pitr_03.jpg)

```bash
service mysql stop
rm -rf /var/lib/mysql/*  # Oops we dropped the database!

# restore database
(1) wal-g-mysql --turbo --config .walg.properties backup-fetch LATEST
⏱️ real    1m44.531s

(2) service mysql start

# tell MySQL that this instance doesn't have old binlog files
(3) gtids=$(tr -d '\n' < /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
(4) mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtids';"

# replay binlog files:
time wal-g-mysql --turbo --config .walg.properties binlog-replay --since LATEST --until "2030-01-02T15:04:05Z
⏱️ real    118m3.330s
```

Database recovery took 2 minutes (that was a tiny database!), however PiTR took 118 minutes(!). It is easy to spot that it takes more time than sysbench spent to genereate all this binlogs (50 minutes).

Now, let's consider faster recovery approaches!

# PiTR like a rockstar

![IntroIllustration](resources/mysql_pitr_04.jpg)

This is approached advertised by  Frédéric (Lefred) Descamps[1]. The of this approach is to put binlogs near the MySQL Server and configure it in a way that it will think that it should apply it (as fetched relay logs).

[1] https://lefred.be/content/howto-make-mysql-point-in-time-recovery-faster/

[1] https://www.slideshare.net/lefred.descamps/fosdem-mysql-friends-devroomffebruary-2018-mysql-pointintime-recovery-like-a-rockstar

[1] https://www.youtube.com/watch?v=PinKYCfv1MM&t=1787s

This manual will not cover all steps in details (follow Lefred's instructions[1]), but it may not be trivial to do all these steps in a stressful situation, so do test recoveries regularly.

What is important here - this approach is really fast! (If you can execute all steps quickly!)

Binlog fetching takes (in my scenario) less than 2 minutes! (Network is fast, nowadays!)
```bash
time wal-g-mysql --turbo --config .walg.properties binlog-fetch --since LATEST --until "2030-01-02T15:04:05Z"
⏱️ real    1m39.685s
```
Applying all these binlogs (45 minutes) is also comparable with `sysbench` working time (50 minutes):
```SQL
SET GLOBAL SERVER_ID = 99;
CHANGE REPLICATION SOURCE  TO RELAY_LOG_FILE='mysql1-relay-bin.000057', RELAY_LOG_POS=1, SOURCE_HOST='dummy';
START REPLICA SQL_THREAD;
<replication finished in 45 minutes>
```

We can see, that MySQL applies transactions concurrently (group commit):
```SQL
mysql> SHOW PROCESSLIST
+----+-----------------+-----------+------+---------+-------+---------------------------------------------+------------------+---------+-----------+---------------+
| Id | User            | Host      | db   | Command | Time  | State                                       | Info             | Time_ms | Rows_sent | Rows_examined |
+----+-----------------+-----------+------+---------+-------+---------------------------------------------+------------------+---------+-----------+---------------+
|  5 | event_scheduler | localhost | NULL | Daemon  |   190 | Waiting on empty queue                      | NULL             |  190001 |         0 |             0 |
| 10 | root            | localhost | NULL | Query   |     0 | init                                        | show processlist |       0 |         0 |             0 |
| 35 | system user     |           | NULL | Query   |     0 | Waiting for dependent transaction to commit | NULL             |       0 |         0 |             0 |
| 36 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 37 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 38 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 39 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 40 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 41 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 42 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
| 43 | system user     |           | test | Query   | 19129 | Applying batch of row changes (update)      | NULL             |       0 |         0 |             0 |
+----+-----------------+-----------+------+---------+-------+---------------------------------------------+------------------+---------+-----------+---------------+
```

## Greatest PiTR since sliced bread

![IntroIllustration](resources/mysql_pitr_05.jpg)

Is it possible to recovery quickly without digging into bash? Yep! Let's use `wal-g binlog-server`. It will simulate MySQL primary host and will serve binlogs from S3 to your MySQL. It will take care of decryption and decompression and will do all magic for you  (almost).

We should add couple config options to mysql.conf
```
relay_log_space_limit=8589934592  # force MySQL to remove unneded relay logs
skip-slave-start  # don't try to replicate from old primary (if it is still alive)
```
And add following lines to walg.properties:
```
WALG_MYSQL_BINLOG_SERVER_HOST=localhost
WALG_MYSQL_BINLOG_SERVER_PORT=9306
WALG_MYSQL_BINLOG_SERVER_USER=walg
WALG_MYSQL_BINLOG_SERVER_PASSWORD=walgpwd
WALG_MYSQL_BINLOG_SERVER_ID=99

WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE=root:Qwerty12345@127.0.0.1:3306/test
```

Now, recovery process is following:

- restore from backup with `wal-g backup-fetch` (steps (1)...(4) in chapter Classic PiTR)
- start `wal-g binlog-server`
- start MySQL
- replicate from wal-g:
```
SET GLOBAL SERVER_ID=999;
CHANGE MASTER TO MASTER_HOST="127.0.0.1", MASTER_PORT=9306, MASTER_USER="walg", MASTER_PASSWORD="walgpwd", MASTER_AUTO_POSITION=1;
SHOW REPLICA STATUS \G
START REPLICA;
```
- chilling until replication finished (`wal-g binlog-server` exits after MySQL applied transactions)

On my test stand it took 48 minutes, that is quite close to “like a rock-star” approach.

## Summary


| Approach                | PiTR to timestamp | PiTR to binlog position | PiTR to GTID |
|-------------------------|:-----------------:|:-----------------------:|:------------:|
| Classic PiTR            | ✅ [1] | ✅[1] |    ✅ [1]     |
| PiTR like a rock star   | 🛑 | ✅ [2] |    ✅ [3]     |
| PiTR with binlog-server | ✅ | ✅ [2] |   ✅ [3]      |


[1] with different `mysqlbinlog` CLI's arguments 

[2] with `CHANGE MASTER TO … MASTER_LOG_FILE=xxx MASTER_LOG_POS=yyy`

[3] with `START REPLICA … SQL_BEFORE_GTIDS/SQL_AFTER_GTIDS=xxx`


| Approach                 |  PiTR  |   PiTR + hacks   |           Extra space          |
|--------------------------|:------:|:----------------:|:------------------------------:|
| Classic PiTR             | 118min |    65m [3][4]    |        2 binlog files[6]       |
| PiTR like a rock star    | 45min  | 17min [1][2][3]  | all binlogs we should apply [5] |
| PiTR with binlog-server  | 48min  | 18min [2][3][4]  | 2 x relay_log_space_limit [6] |

[1] `log_replica_updates=0` - do not write transaction from relay logs to own binlogs (Note: you should remove this option after recovery & restart mysqld) [MySQL 8.0.26+]

[2] `binlog_transaction_dependency_tracking=WRITESET` - increase concurrency

[3] disable redo log: `ALTER INSTANCE DISABLE INNODB REDO_LOG;` (it is not crash-safe!)

[4] `sync_binlog=3000` - call fsync less (it is not crash safe!)

[5] or even x2 of all binlogs if `log_replica_updates=0` is not set

[6] I suppose, that you are removing binlogs by your cron job (don't rely on `binlog_expire_logs_seconds`)?

## Kudos

- https://commitstrip.com/ - kudos for comics idea
- @Fizic - kudos for binlog-server implementation

---
## Technical Section

`wal-g` binlog-server replication is built on top of github.com/go-mysql-org/go-mysql.
We use custom `server.Handler` implementation in `internal/databases/mysql/binlog_server_handler.go`.
That basically just responds to MySQL request:
 * when `COM_QUERY` message received - it may respond to some predefined set of SQL queries
 * when `COM_BINLOG_DUMP` message received - it streams binlog files starting from requested timestamp (in our case it is backup creation time) until time passed to `--until` cli option
 * when `COM_BINLOG_DUMP_GTID` message received - it streams binlog file (whole file, MySQL knows max GTID, and will not apply unnecessary transactions)

It creates background thread that fetches & decrypt & decompress binlogs and stores it in `WALG_MYSQL_BINLOG_DST` directory
Then `wal-g` reads binlogs, and do the following:
 * sends fake Rotate Event, to override server_id of the binlog file(??)
 * then it shallow-parse binlog file and sends binlog events one by one to MySQL.
 * when it sees `GTID_EVENT` with time of creation < `--until` time, it stores this GTID as 'last seen GTOD' in order to wait until MySQL apply it.