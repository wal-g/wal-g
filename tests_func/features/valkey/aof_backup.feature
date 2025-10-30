#noinspection CucumberUndefinedStep
Feature: Valkey AOF backups check

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a working valkey on valkey01
    And a working valkey on valkey02
    And a configured s3 on minio01

  Scenario: Backups, restores and deletes were done successfully
    When valkey01 has test valkey data test1
    And valkey01 manifest is not empty
    And we create valkey01 aof-valkey-backup with success
    Then we got 1 backup entries of valkey01

    # Successfully create backup on constantly changing AOF increment file
    When valkey01 has test valkey data test2
    And valkey01 has heavy write
    And we create valkey01 aof-valkey-backup with success
    And we stop heavy write on valkey01
    Then we got 2 backup entries of valkey01

    # Backup could not be created if disk is full above limit
    And we create valkey01 aof-valkey-backup with threshold
    Then we got 2 backup entries of valkey01

    When valkey01 has test valkey data test3
    And we create valkey01 aof-valkey-backup with success
    Then we got 3 backup entries of valkey01

    When we put empty backup via minio01 to valkeydump.archive
    Then we got 3 backup entries of valkey01

    # Backups purged successfully
    When we delete valkey backups retain 2 via valkey01
    Then we got 2 backup entries of valkey01
    And we check if empty backups were purged via minio01

    # Second purge does not delete backups
    When we delete valkey backups retain 2 via valkey01
    Then we got 2 backup entries of valkey01

    # Last backup restored successfully
    When we stop valkey-server at valkey02
    And we restore #1 aof same version backup to valkey02
    Then valkey stopped on valkey02
    When we start valkey-server at valkey02
    And a working valkey on valkey02
    Then we got same valkey data at valkey01 valkey02

    # Last backup restored with wrong version fails
    When we stop valkey-server at valkey02
    And we restore #1 aof wrong version backup to valkey02
    Then valkey stopped on valkey02
    When we start valkey-server at valkey02
    And a working valkey on valkey02
    Then we got same valkey data at valkey01 valkey02

    # Pre-last backup restored successfully
    When we stop valkey-server at valkey02
    And we restore #0 aof same version backup to valkey02
    Then valkey stopped on valkey02
    When we start valkey-server at valkey02
    And a working valkey on valkey02
    # we have nothing to compare with as #0 backup was made under load
