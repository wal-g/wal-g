Feature: Labels backups check

  Background: Wait for working infrastructure
    Given a working mongodb on mongodb01
    And a configured s3 on minio01
    And mongodb replset initialized on mongodb01
    And mongodb auth initialized on mongodb01

  Scenario: Backup with Labels done successfully
    When we create mongodb01 backup with user data
    """
    labels:
      name: test_backup
    """
    Then we ensure mongodb01 #0 backup metadata contains
    """
    labels:
      name: test_backup
    """