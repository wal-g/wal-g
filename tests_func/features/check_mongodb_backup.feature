Feature: MongoDB backups check

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

  Scenario: First backup was done successfully
    Given mongodb01 has test mongodb data test1
    When we create mongodb01 backup
    Then we got 1 backup entries of mongodb01

  Scenario: Second backup was done successfully
    Given mongodb01 has test mongodb data test2
    When we create mongodb01 backup
    Then we got 2 backup entries of mongodb01

  Scenario: Third backup was done successfully
    Given mongodb01 has test mongodb data test3
    When we create mongodb01 backup
    Then we got 3 backup entries of mongodb01

  Scenario: Fourth backup was done successfully
    Given mongodb01 has test mongodb data test4
    When we create mongodb01 backup
    Then we got 4 backup entries of mongodb01
    When we put empty backup via minio01
    Then we got 4 backup entries of mongodb01

  Scenario: Backups purged successfully
    When we delete backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01
    And we check if empty backups were purged via minio01

  Scenario: Second purge does not delete backups
    When we delete backups retain 3 via mongodb01
    Then we got 3 backup entries of mongodb01

  Scenario: Backup1 restored successfully
    When we restore #0 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Backup2 restored successfully
    When we restore #1 backup to mongodb01
    And we restore #1 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Fifth backup was done successfully
    Given mongodb01 has test mongodb data test5
    When we create mongodb01 backup
    And we wait for 1 seconds
    And we create timestamp #0 via mongodb01
    Then we got 4 backup entries of mongodb01

  Scenario: Backups delete1 successfully
    When we delete backups retain 2 after #3 backup via mongodb01
    Then we got 4 backup entries of mongodb01

  Scenario: Backup3 restored successfully
    When we restore #0 backup to mongodb01
    And we restore #0 backup to mongodb02
    Then we got same mongodb data at mongodb01 mongodb02

  Scenario: Six backup was done successfully
    Given mongodb01 has test mongodb data test6
    When we create mongodb01 backup
    Then we got 5 backup entries of mongodb01

  Scenario: Seventh backup was done successfully
    Given mongodb01 has test mongodb data test7
    When we create mongodb01 backup
    Then we got 6 backup entries of mongodb01

  Scenario: Backups delete2 successfully
    When we delete backups retain 4 after #0 timestamp via mongodb01
    Then we got 4 backup entries of mongodb01
