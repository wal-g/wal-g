#noinspection CucumberUndefinedStep
Feature: MongoDB binary backups

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And mongodb initialized on mongodb02

  Scenario: Binary backups, restores and deletes were done successfully
    When mongodb01 has test mongodb data test1
    And we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01

    When mongodb01 has test mongodb data test2
    And we create binary mongo-backup on mongodb01
    Then we got 2 backup entries of mongodb01

    When mongodb01 has test mongodb data test3
    And we create binary mongo-backup on mongodb01
    Then we got 3 backup entries of mongodb01

    When mongodb01 has test mongodb data test4
    And we create binary mongo-backup on mongodb01
    Then we got 4 backup entries of mongodb01

    When we put empty backup via minio01 to mongodump.archive
    Then we got 4 backup entries of mongodb01

    # Backups purged successfully
    When we delete mongo backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01
    And we check if empty backups were purged via minio01

    # Second purge does not delete backups
    When we delete mongo backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01

    # Last backup restored successfully
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore binary mongo-backup #2 to mongodb02
    Then mongodb role is primary on mongodb02
    And we got same mongodb data at mongodb01 mongodb02

    # Pre-last backup restored successfully
    Given mongodb01 has no data
    And mongodb02 has no data
    And mongodb initialized on mongodb02
    And mongodb initialized on mongodb01
    When we restore binary mongo-backup #1 to mongodb01
    And we restore binary mongo-backup #1 to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

    # Restore initialized
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore initialized binary mongo-backup #1 to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

    # Fifth backup was done successfully
    Given mongodb01 has test mongodb data test5
    When we create binary mongo-backup on mongodb01
    Then we got 4 backup entries of mongodb01

    # Forth and first backup were deleted successfully
    When we delete mongo backup #3 via mongodb01
    Then we got 3 backup entries of mongodb01
    When we delete mongo backup #0 via mongodb01
    Then we got 2 backup entries of mongodb01
    When we purge oplog archives via mongodb01
