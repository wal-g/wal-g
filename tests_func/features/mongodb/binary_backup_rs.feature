Feature: MongoDB binary restore replSet

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And mongodb initialized on mongodb02

  Scenario: Binary backups, restore replSet and deletes was done successfully
    Given mongodb01 has test mongodb data test1
    When we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01

    # Restore rs from backup and check no initial sync
    Given mongodb01 has no data
    And mongodb02 has no data
    And mongodb initialized on mongodb02
    And mongodb initialized on mongodb01
    When we restore rs from binary mongo-backup #0 to mongodb01,mongodb02
    Then mongodb not has initial sync on mongodb01
    And mongodb not has initial sync on mongodb02

    # Create backup for checking
    When we create binary mongo-backup on mongodb01
    And we create binary mongo-backup on mongodb02
    Then we got 3 backup entries of mongodb01

    # Check same data
    Given mongodb01,mongodb02 replset has no data
    And mongodb initialized on mongodb02
    And mongodb initialized on mongodb01
    When we restore binary mongo-backup #1 to mongodb01
    And we restore binary mongo-backup #2 to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02
