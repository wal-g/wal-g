#noinspection CucumberUndefinedStep
Feature: MongoDB backups check

  Background: Wait for working infrastructure
    Given a working mongodb on mongodb01
    And a working mongodb on mongodb02
    And a configured s3 on minio01
    And mongodb replset initialized on mongodb01
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb01
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb01
    And mongodb role is primary on mongodb02

  Scenario: Backups were done successfully
    When mongodb01 has test mongodb data test1
    And we create mongodb01 backup
    Then we got 1 backup entries of mongodb01

    When mongodb01 has test mongodb data test2
    And we create mongodb01 backup
    Then we got 2 backup entries of mongodb01

    When mongodb01 has test mongodb data test3
    And we create mongodb01 backup
    Then we got 3 backup entries of mongodb01

    When mongodb01 has test mongodb data test4
    And we create mongodb01 backup
    Then we got 4 backup entries of mongodb01

    When we put empty backup via minio01
    Then we got 4 backup entries of mongodb01

  Scenario: Backups purged successfully
    When we delete backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01
    And we check if empty backups were purged via minio01

  Scenario: Second purge does not delete backups
    When we delete backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01

  Scenario: Last backup restored successfully
    When we restore #2 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Pre-last backup restored successfully
    When we restore #1 backup to mongodb01
    And we restore #1 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Fifth backup was done successfully
    Given mongodb01 has test mongodb data test5
    When we create mongodb01 backup
    Then we got 4 backup entries of mongodb01

  Scenario: Forth and first backup were deleted successfully
    When we delete backup #3 via mongodb01
    Then we got 3 backup entries of mongodb01
    When we delete backup #0 via mongodb01
    Then we got 2 backup entries of mongodb01
