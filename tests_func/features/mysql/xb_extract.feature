Feature: xtrabackup tools tests

  Background: Wait for working infrastructure
    Given prepared infrastructure
    And a configured s3 on minio01

  Scenario: xb tools extract
    Then bash script "/testdata/mysql/xtrabackup_extract.sh" executed on host "mysql01" finished with result '0'