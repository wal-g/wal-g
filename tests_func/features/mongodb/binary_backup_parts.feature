#noinspection CucumberUndefinedStep
Feature: MongoDB binary backups with split collection and index files

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on seaweedfs01
    And mongodb initialized on mongodb01
    And mongodb initialized on mongodb02

  Scenario: Many small encrypted file parts restore all documents and indexes
    Given mongodb01 has 12000 indexed large mongodb documents
    When we create binary mongo-backup on mongodb01 with file parts of 65536 bytes
    Then we got 1 backup entries of mongodb01
    And binary mongo-backup #0 on mongodb01 stores collection and indexes in parts of 65536 bytes

    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore binary mongo-backup #0 to mongodb02
    Then mongodb role is primary on mongodb02
    And mongodb02 has 12000 valid indexed mongodb documents
    And we got same mongodb data at mongodb01 mongodb02

    Given mongodb02 has no data
    And mongodb initialized on mongodb02
    When we restore mongo-backup #0 to mongodb02 with whitelist "file_parts.documents"
    Then mongodb02 has only db "file_parts" and col "documents"
    And mongodb02 has 12000 valid indexed mongodb documents
    And mongodb01 and mongodb02 has same data in db "file_parts" and col "documents"
