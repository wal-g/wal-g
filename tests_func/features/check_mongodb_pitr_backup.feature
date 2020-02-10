Feature: MongoDB PITR backups check

  Background: Wait for working infrastructure
    Given a configured s3 on minio01
    And a working mongodb on mongodb01
    And a working mongodb on mongodb02
    And mongodb replset initialized on mongodb01
    And mongodb auth initialized on mongodb01
    And mongodb role is primary on mongodb01
    And oplog archiving is enabled on mongodb01
    And at least one oplog archive exists in storage


  Scenario: First backup was done successfully
    Given mongodb01 has test mongodb data test1
    When we create mongodb01 backup
    Then we got 1 backup entries of mongodb01


  Scenario: First load
    Given mongodb01 has test mongodb data test2
    And we save last oplog timestamp on mongodb01 to "after first load"
    And we save mongodb01 data "after first load"


  Scenario: Second backup was done successfully
    When we create mongodb01 backup
    Then we got 2 backup entries of mongodb01


  Scenario: Second load
    Given mongodb01 has test mongodb data test3
    And we save last oplog timestamp on mongodb01 to "after second load"
    And we save mongodb01 data "after second load"
    Then we got 2 backup entries of mongodb01


  Scenario: PITR: 1st backup to 1st ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #0 backup to mongodb02
    And we restore from #0 backup to "after first load" timestamp to mongodb02
    And we save mongodb02 data "restore to after first load from second backup"
    Then we have same data in "after first load" and "restore to after first load from second backup"


  Scenario: PITR: 2nd backup to 2nd ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #1 backup to mongodb02
    And we restore from #1 backup to "after second load" timestamp to mongodb02
    And we save mongodb02 data "restore to after second load from second backup"
    Then we have same data in "after second load" and "restore to after second load from second backup"


  Scenario: PITR: 1st backup to 2st ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #0 backup to mongodb02
    And we restore from #0 backup to "after second load" timestamp to mongodb02
    And we save mongodb02 data "restore to after second load from first backup"
    Then we have same data in "after second load" and "restore to after second load from first backup"
