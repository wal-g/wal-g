#noinspection CucumberUndefinedStep
Feature: Redis AOF backups check

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a working redis on redis01
    And a working redis on redis02
    And a configured s3 on minio01

  Scenario: Backups, restores and deletes were done successfully
    When redis01 has test redis data test1
    And redis01 manifest is not empty
    And we create redis01 aof-redis-backup with success
    Then we got 1 backup entries of redis01

    # Successfully create backup on constantly changing AOF increment file
    When redis01 has test redis data test2
    And redis01 has heavy write
    And we create redis01 aof-redis-backup with success
    And we stop heavy write on redis01
    Then we got 2 backup entries of redis01

    # Backup could not be created if disk is full above limit
    And we create redis01 aof-redis-backup with threshold
    Then we got 2 backup entries of redis01

    When redis01 has test redis data test3
    And we create redis01 aof-redis-backup with success
    Then we got 3 backup entries of redis01

    When we put empty backup via minio01 to redisdump.archive
    Then we got 3 backup entries of redis01

    # Backups purged successfully
    When we delete redis backups retain 2 via redis01
    Then we got 2 backup entries of redis01
    And we check if empty backups were purged via minio01

    # Second purge does not delete backups
    When we delete redis backups retain 2 via redis01
    Then we got 2 backup entries of redis01

    # Last backup restored successfully
    When we stop redis-server at redis02
    And we restore #1 aof same version backup to redis02
    Then redis stopped on redis02
    When we start redis-server at redis02
    And a working redis on redis02
    Then we got same redis data at redis01 redis02

    # Last backup restored with wrong version fails
    When we stop redis-server at redis02
    And we restore #0 aof wrong version backup to redis02
    Then redis stopped on redis02
    When we start redis-server at redis02
    And a working redis on redis02
    Then we got same redis data at redis01 redis02

    # Pre-last backup restored successfully
    When we stop redis-server at redis01
    And we restore #0 aof same version backup to redis01
    Then redis stopped on redis01
    When we stop redis-server at redis02
    And we restore #0 aof same version backup to redis02
    Then redis stopped on redis02
    When we start redis-server at redis01
    And we start redis-server at redis02
    And a working redis on redis01
    And a working redis on redis02
    Then we got same redis data at redis01 redis02
