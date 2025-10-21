#noinspection CucumberUndefinedStep
Feature: Valkey RDB backups check

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a working valkey on valkey01
    And a working valkey on valkey02
    And a configured s3 on minio01

  Scenario: Backups, restores and deletes were done successfully
    When valkey01 has test valkey data test1
    And we create valkey01 rdb-valkey-backup with success
    Then we got 1 backup entries of valkey01

    When valkey01 has test valkey data test2
    And we create valkey01 rdb-valkey-backup with success
    Then we got 2 backup entries of valkey01

    When valkey01 has test valkey data test3
    And we create valkey01 rdb-valkey-backup with success
    Then we got 3 backup entries of valkey01

    When valkey01 has test valkey data test4
    And we create valkey01 rdb-valkey-backup with success
    Then we got 4 backup entries of valkey01

    When we put empty backup via minio01 to valkeydump.archive
    Then we got 4 backup entries of valkey01

    # Backups purged successfully
    When we delete valkey backups retain 3 via valkey01
    Then we got 3 backup entries of valkey01
    And we check if empty backups were purged via minio01

    # Second purge does not delete backups
    When we delete valkey backups retain 3 via valkey01
    Then we got 3 backup entries of valkey01

    # Last backup restored successfully
    When we restore #2 backup to valkey02
    And we restart valkey-server at valkey02
    And a working valkey on valkey02
    Then we got same valkey data at valkey01 valkey02

    # Pre-last backup restored successfully
    When we restore #1 backup to valkey01
    And we restore #1 backup to valkey02
    And we restart valkey-server at valkey01
    And we restart valkey-server at valkey02
    And a working valkey on valkey01
    And a working valkey on valkey02
    Then we got same valkey data at valkey01 valkey02

    # Fifth backup was done successfully
    Given valkey01 has test valkey data test5
    When we create valkey01 rdb-valkey-backup with success
    Then we got 4 backup entries of valkey01
