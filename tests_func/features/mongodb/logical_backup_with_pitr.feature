#noinspection CucumberUndefinedStep
Feature: MongoDB PITR backups check

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And a working mongodb on mongodb01
    And a working mongodb on mongodb02
    And mongodb replset initialized on mongodb01
    And mongodb auth initialized on mongodb01
    And mongodb role is primary on mongodb01
    And oplog archiving is enabled on mongodb01
    And at least one oplog archive exists in storage


  Scenario: Backup with pitr was done successfully
    Given mongodb01 has test mongodb data test1
    When we create mongodb01 mongo-backup
    Then we got 1 backup entries of mongodb01


    # First load
    Given mongodb01 has been loaded with "load1"
    And we save last oplog timestamp on mongodb01 to "after first load"
    And we save mongodb01 data "after first load"


    # Second backup was done successfully
    When we create mongodb01 mongo-backup
    Then we got 2 backup entries of mongodb01


    # Second load
    Given mongodb01 has been loaded with "load2"
    And we save last oplog timestamp on mongodb01 to "after second load"
    And we save mongodb01 data "after second load"


    #: Third load
    Given mongodb01 has been loaded with "load3"
    And we save last oplog timestamp on mongodb01 to "after third load"
    And we save mongodb01 data "after third load"

    #: Fourth load
    Given mongodb01 has been loaded with "load4"
    And we save last oplog timestamp on mongodb01 to "after fourth load"
    And we save mongodb01 data "after fourth load"


    # PITR: 1st backup to 1st ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #0 backup to mongodb02
    And we restore from #0 backup to "after first load" timestamp to mongodb02
    And we save mongodb02 data "restore to after first load from first backup"
    Then we have same data in "after first load" and "restore to after first load from first backup"


    # PITR: 2nd backup to 2nd ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #1 backup to mongodb02
    And we restore from #1 backup to "after second load" timestamp to mongodb02
    And we save mongodb02 data "restore to after second load from second backup"
    Then we have same data in "after second load" and "restore to after second load from second backup"


    # PITR: 1st backup to 2nd ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #0 backup to mongodb02
    And we restore from #0 backup to "after second load" timestamp to mongodb02
    And we save mongodb02 data "restore to after second load from first backup"
    Then we have same data in "after second load" and "restore to after second load from first backup"


    # PITR: 2nd backup to 3rd ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #1 backup to mongodb02
    And we restore from #1 backup to "after third load" timestamp to mongodb02
    And we save mongodb02 data "restore to after third load from second backup"
    Then we have same data in "after second load" and "restore to after second load from second backup"

    # PITR: 2nd backup to 4th ts
    Given mongodb02 has no data
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb02

    When we restore #1 backup to mongodb02
    And we restore from #1 backup to "after fourth load" timestamp to mongodb02
    And we save mongodb02 data "restore to after fourth load from second backup"
    Then we have same data in "after fourth load" and "restore to after fourth load from second backup"
