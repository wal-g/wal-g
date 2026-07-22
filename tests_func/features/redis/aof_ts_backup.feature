Feature: Redis AOF plus tiered-storage backup

  Background:
    Given prepared infrastructure
    And a working redis on redis01
    And a configured s3 on minio01

  Scenario: AOF TS backup restores Redis and TS data together on one node
    Given redis01 has test redis data test1
    And redis01 manifest is not empty
    And redis01 has a frozen ts tree at /data/aof-ts-source
    And redis02 has a frozen ts tree at /data/aof-ts-expected
    When we create redis01 aof_ts tiered backup from /data/aof-ts-source
    Then we got 1 backup entries of redis01
    When we stop redis-server at redis02
    And we fetch latest aof_ts backup from redis01 on redis02 into /data/aof-ts-restore
    And we start redis-server at redis02
    And a working redis on redis02
    Then we got same redis data at redis01 redis02
    And the ts tree at /data/aof-ts-expected matches /data/aof-ts-restore on redis02
    When we delete latest redis backup via redis01
    Then we got 0 backup entries of redis01
