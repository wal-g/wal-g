#noinspection CucumberUndefinedStep
Feature: Redis backups check

  Background: Wait for working infrastructure
    Given a working redis on redis01
    And a working redis on redis02
    And a configured s3 on minio01

  Scenario: Backups were done successfully
    When redis01 has test redis data test1
    And we create redis01 redis-backup
    Then we got 1 backup entries of redis01

    When redis01 has test redis data test2
    And we create redis01 redis-backup
    Then we got 2 backup entries of redis01

    When redis01 has test redis data test3
    And we create redis01 redis-backup
    Then we got 3 backup entries of redis01

    When redis01 has test redis data test4
    And we create redis01 redis-backup
    Then we got 4 backup entries of redis01

    When we put empty backup via minio01 to redisdump.archive
    Then we got 4 backup entries of redis01

  Scenario: Backups purged successfully
    When we delete redis backups retain 3 via redis01
    Then we got 3 backup entries of redis01
    And we check if empty backups were purged via minio01

  Scenario: Second purge does not delete backups
    When we delete redis backups retain 3 via redis01
    Then we got 3 backup entries of redis01

  Scenario: Last backup restored successfully
    When we restore #2 backup to redis02
    And we restart redis-server at redis02
    And a working redis on redis02
    Then we got same redis data at redis01 redis02

  Scenario: Pre-last backup restored successfully
    When we restore #1 backup to redis01
    And we restore #1 backup to redis02
    And we restart redis-server at redis01
    And we restart redis-server at redis02
    And a working redis on redis01
    And a working redis on redis02
    Then we got same redis data at redis01 redis02

  Scenario: Fifth backup was done successfully
    Given redis01 has test redis data test5
    When we create redis01 redis-backup
    Then we got 4 backup entries of redis01
