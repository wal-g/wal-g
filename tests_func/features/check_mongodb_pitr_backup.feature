Feature: MongoDB PITR backups check

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
    And oplog archive is on mongodb01

  Scenario: First backup was done successfully
    Given mongodb01 has test mongodb data test1
    And we wait for 15 seconds
    When we create timestamp "before first backup" via mongodb01
    And we wait for 1 seconds
    And we create mongodb01 backup
    Then we got 1 backup entries of mongodb01

  Scenario: First database data saved successfully
    When we load mongodb01 with "load/small_insert_load_config_1.json" config
    And we wait for 15 seconds
    And we create timestamp "after first load" via mongodb01
    And we save mongodb01 data "#0"
    Then we got 1 backup entries of mongodb01

  Scenario: Second backup was done successfully
    Given mongodb01 has test mongodb data test2
    And we wait for 15 seconds
    When we create timestamp "before second backup" via mongodb01
    And we wait for 1 seconds
    When we create mongodb01 backup
    Then we got 2 backup entries of mongodb01

  Scenario: Second database data saved successfully
    When we load mongodb01 with "load/small_insert_load_config_2.json" config
    And we wait for 15 seconds
    And we create timestamp "after second load" via mongodb01
    And we save mongodb01 data "#1"
    Then we got 2 backup entries of mongodb01

  Scenario: First "ASD" restored successfully
    Given mongodb02 has no data
    When we restore #1 backup to mongodb02
    And we wait for 15 seconds
    And we restore from "before first backup" timestamp to "after first load" timestamp to mongodb02
    And we wait for 15 seconds
    And we save mongodb02 data "#2"
    Then we have same data in "#0" and "#2"

  Scenario: Second "ASD" restored successfully
    Given mongodb02 has no data
    When we restore #1 backup to mongodb02
    And we wait for 15 seconds
    And we restore from "before first backup" timestamp to "after second load" timestamp to mongodb02
    And we wait for 15 seconds
    And we save mongodb02 data "#3"
    Then we have same data in "#1" and "#3"

   #TODO: Support this scenario
  Scenario: Third "ASD" restored successfully
    Given mongodb02 has no data
    When we restore #0 backup to mongodb02
    And we wait for 20 seconds
    And we restore from "before second backup" timestamp to "after second load" timestamp to mongodb02
    And we wait for 20 seconds
    And we save mongodb02 data "#4"
    Then we have same data in "#1" and "#4"