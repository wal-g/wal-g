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

    # Pre-last backup restored successfully
    Given mongodb01 has no data
    And mongodb02 has no data
    And mongodb initialized on mongodb02
    And mongodb initialized on mongodb01
    When we restore rs from binary mongo-backup #0 to mongodb01,mongodb02
