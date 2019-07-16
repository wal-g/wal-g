Feature: Custom backup names check

  Background: Wait for working infrastructure
    Given a working mongodb on mongodb01
      And a configured s3 on minio01
      And mongodb replset initialized on mongodb01
      And mongodb auth initialized on mongodb01
      And a trusted gpg keys on mongodb01

  Scenario: Backup with custom name done successfully
    When we create mongodb01 backup
    """
    name: test_backup1
    """
    And wait for 1 seconds
    When we create mongodb01 backup
    And wait for 1 seconds
    When we create mongodb01 backup
    """
    name: test_backup2
    """
    And wait for 1 seconds
    When we create mongodb01 backup
    Then backup list of mongodb01 is
    """
    - {{safe_storage['created_backup_names'][-1]}}
    - test_backup2
    - {{safe_storage['created_backup_names'][-3]}}
    - test_backup1
    """
