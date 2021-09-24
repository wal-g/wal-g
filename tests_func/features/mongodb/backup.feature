#noinspection CucumberUndefinedStep
Feature: MongoDB backups check

  Background: Wait for working infrastructure
    Given mongodb initialized on mongodb01
    And mongodb initialized on mongodb02
    And a configured s3 on minio01

  Scenario: Backups were done successfully
    When mongodb01 has test mongodb data test1
    And we create mongodb01 mongo-backup
    Then we got 1 backup entries of mongodb01

    When mongodb01 has test mongodb data test2
    And we create mongodb01 mongo-backup
    Then we got 2 backup entries of mongodb01

    When mongodb01 has test mongodb data test3
    And we create mongodb01 mongo-backup
    Then we got 3 backup entries of mongodb01

    When mongodb01 has test mongodb data test4
    And we create mongodb01 mongo-backup
    Then we got 4 backup entries of mongodb01

    When we put empty backup via minio01 to mongodump.archive
    Then we got 4 backup entries of mongodb01

  Scenario: Backups purged successfully
    When we delete mongo backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01
    And we check if empty backups were purged via minio01

  Scenario: Second purge does not delete backups
    When we delete mongo backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01

  Scenario: Last backup restored successfully
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore #2 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Pre-last backup restored successfully
    Given mongodb01 has no data
    And mongodb02 has no data
    And mongodb initialized on mongodb02
    And mongodb initialized on mongodb01
    When we restore #1 backup to mongodb01
    And we restore #1 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Fifth backup was done successfully
    Given mongodb01 has test mongodb data test5
    When we create mongodb01 mongo-backup
    Then we got 4 backup entries of mongodb01

  Scenario: Forth and first backup were deleted successfully
    When we delete mongo backup #3 via mongodb01
    Then we got 3 backup entries of mongodb01
    When we delete mongo backup #0 via mongodb01
    Then we got 2 backup entries of mongodb01
    When we purge oplog archives via mongodb01
