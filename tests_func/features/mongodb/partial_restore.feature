#noinspection CucumberUndefinedStep
Feature: MongoDB partially restore

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And mongodb01 has partial test mongodb data
    And we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01

  Scenario: Partial restore works
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore partial mongo-backup #0 to mongodb02 ns "part1.col2"
    And a working mongodb on mongodb02
    Then mongodb02 has only db "part1" and col "col2"
    And mongodb01 and mongodb02 has same data in db "part1" and col "col2"

  Scenario: Partial restore with blacklist works
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore partial mongo-backup #0 to mongodb02 ns "part1" with blacklist "part1.col2"
    And a working mongodb on mongodb02
    Then mongodb02 has only db "part1" and col "col1"
    And mongodb01 and mongodb02 has same data in db "part1" and col "col1"

  Scenario: Partial restore fails with unexistent db
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore partial mongo-backup #0 to mongodb02 ns "unexistent_db" with error "No db unexistent_db in backup"

  Scenario: Partial restore fails with unexistent collection
    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore partial mongo-backup #0 to mongodb02 ns "part1.unexistent_col" with error "No collection unexistent_col in db part1 in backup"
