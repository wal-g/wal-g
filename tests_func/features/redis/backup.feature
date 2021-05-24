#noinspection CucumberUndefinedStep
Feature: Redis backups check

  Background: Wait for working infrastructure
    Given a working redis on redis01
    And a working redis on redis02
    And a configured s3 on minio01

  Scenario: Backups were done successfully
    When redis01 has test redis data test1
    And we create redis01 redis-backup
    Then we got 1 backup entries of redis01