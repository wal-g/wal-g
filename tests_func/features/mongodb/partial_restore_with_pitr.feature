#noinspection CucumberUndefinedStep
Feature: MongoDB binary backups restores partially with PITR

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And oplog archiving is enabled on mongodb01
    And at least one oplog archive exists in storage

  Scenario: Binary backups, and partial restores with pitr was done successfully
    # first load to backup
    And mongodb01 has been loaded with "partial1"
    When we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01
    And journal info count is #1
    # second load to pitr
    Given mongodb01 has been loaded with "partial2"
    And we save last oplog timestamp on mongodb01 to "after second load"
    And we save mongodb01 data "after second load"
    ###
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    # partial restore only with partial.coll
    When we restore mongo-backup #0 to mongodb02 with whitelist "partial.coll"
    And a working mongodb on mongodb02
    Then mongodb02 has only db "partial" and col "coll"
    # partial oplog-replay, it may contains other namespaces cause of oplog notes
    # that's why we check equality only in partial.coll
    And we restore from #0 backup to "after second load" timestamp to mongodb02 partial
    And mongodb01 and mongodb02 has same data in db "partial" and col "coll"