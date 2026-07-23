Feature: Redis RDB plus tiered-storage backup

  Background:
    Given prepared infrastructure
    And a working redis on redis01
    And a configured s3 on minio01

  Scenario: RDB TS backup restores Redis and TS data together on one node
    Given redis01 has test redis data test1
    And redis01 has a frozen ts tree at /data/rdb-ts-source
    And redis02 has a frozen ts tree at /data/rdb-ts-expected
    When we create redis01 rdb_ts tiered backup from /data/rdb-ts-source
    Then we got 1 backup entries of redis01
    When we fetch latest rdb_ts backup from redis01 on redis02 into /data/rdb-ts-restore
    And we restart redis-server at redis02
    And a working redis on redis02
    Then we got same redis data at redis01 redis02
    And the ts tree at /data/rdb-ts-expected matches /data/rdb-ts-restore on redis02
    When we delete latest redis backup via redis01
    Then we got 0 backup entries of redis01

  Scenario: RDB TS failure removes the whole combined backup
    Given redis01 has test redis data test1
    And redis01 has a frozen ts tree at /dev/shm/rdb-ts-cross-device-source
    When we create redis01 rdb_ts tiered backup from /dev/shm/rdb-ts-cross-device-source and it fails
    Then we got 0 backup entries of redis01
