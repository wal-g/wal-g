#noinspection CucumberUndefinedStep
Feature: MongoDB partially restore

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And mongodb initialized on mongodb02

  Scenario: Partially restore works correctly
    When mongodb01 has partially test mongodb data
    And we create binary mongo-backup on mongodb01
    Then we got 1 backup entries of mongodb01

    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore initialized binary mongo-backup #0 to mongodb02 with parts "part1.col2"
    Then we got same mongodb data at mongodb01 mongodb02

