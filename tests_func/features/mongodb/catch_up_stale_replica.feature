#noinspection CucumberUndefinedStep
Feature: MongoDB catch up stale replica with oplog replay
  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01
    And mongodb initialized on mongodb01
    And oplog archiving is enabled on mongodb01
    And at least one oplog archive exists in storage

  Scenario: Stop replica, fill master's oplog, restart replica as standalone, replay oplog, return replica to replicaset
    And mongodb replica mongodb02 initialized with mongodb01 master
    And mongodb01 has been loaded with "load1"

    And we stop mongo on mongodb02
    And we fill oplog on mongodb01

    When we run oplog-replay on mongodb02
    Then we got same mongodb data at mongodb01 mongodb02