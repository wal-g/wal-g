Feature: Redis standalone tiered-storage backup

  Background:
    Given prepared infrastructure
    And a working redis on redis01
    And a configured s3 on minio01

  Scenario: Standalone TS backup is listed, fetched, and deleted
    Given redis01 has a frozen ts tree at /data/ts-source
    When we create redis01 ts tiered backup from /data/ts-source
    Then we got 1 backup entries of redis01
    When we fetch latest ts backup from redis01 into /data/ts-restore
    Then the ts tree at /data/ts-source matches /data/ts-restore on redis01
    When we delete latest redis backup via redis01
    Then we got 0 backup entries of redis01

  Scenario: Pinning keeps a TS upload readable after the source is removed
    Given redis01 has a frozen ts tree at /data/ts-source
    And we copy the ts tree from /data/ts-source to /data/ts-expected on redis01
    And we remove /data/ts-source during the next tiered backup on redis01
    When we create redis01 ts tiered backup from /data/ts-source
    Then we got 1 backup entries of redis01
    When we fetch latest ts backup from redis01 into /data/ts-restore
    Then the ts tree at /data/ts-expected matches /data/ts-restore on redis01
