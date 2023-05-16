Feature: MongoDB binary restore replSet with PITR

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And oplog archiving is enabled on mongodb01
    And at least one oplog archive exists in storage
    And mongodb initialized on mongodb02

  Scenario: Binary backups, restore replSet and deletes was done successfully
    Given mongodb01 has test mongodb data test1
    When we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01

    # First load
    Given mongodb01 has been loaded with "load1"
    And we save last oplog timestamp on mongodb01 to "after first load"
    And let's wait new oplog after "after first load"

    # Restore rs from backup and check no initial sync
    Given mongodb01 has no data
    And mongodb02 has no data
    And mongodb initialized on mongodb02
    And mongodb initialized on mongodb01
    When we restore rs from binary mongo-backup #0 to mongodb01,mongodb02
    And mongodb replset is synchronized on mongodb01,mongodb02
    And we restore rs from #0 backup to "after first load" timestamp to mongodb01,mongodb02
    And mongodb replset is synchronized on mongodb01,mongodb02
    Then mongodb doesn't have initial sync on mongodb01
    And mongodb doesn't have initial sync on mongodb02

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
