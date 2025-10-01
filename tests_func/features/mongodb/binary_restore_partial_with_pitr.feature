#noinspection CucumberUndefinedStep
Feature: MongoDB binary backupsrestores partially with PITR

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And oplog archiving is enabled on mongodb01
    And at least one oplog archive exists in storage

  Scenario: Binary backups,  and partial restores with pitr was done successfully
    Given mongodb01 has test mongodb data test1
    And mongodb01 has been loaded with "partial1"
    When we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01
    And journal info count is #1

    Given mongodb01 has been loaded with "partial2"
    And we save last oplog timestamp on mongodb01 to "after second load"
    And we save mongodb01 data "after second load"


    ###
    Given mongodb02 has no data
    And mongodb initialized on mongodb02

    When we restore binary mongo-backup #0 to mongodb02 with whitelist "load1_db.coll"
    And we restore from #0 backup to "after second load" timestamp to mongodb02 with whitelist "load1_db.coll"
    Then mongodb02 has only db "load1_db" and col "coll"
    And mongodb01 and mongodb02 has same data in db "load1_db" and col "coll"