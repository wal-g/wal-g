CREATE EXTENSION IF NOT EXISTS orioledb;

CREATE TABLE pgbench_accounts (
	aid integer NOT NULL,
	bid integer,
	abalance integer,
	filler character(84)
) USING orioledb;

CREATE TABLE pgbench_branches (
	bid integer NOT NULL,
	bbalance integer,
	filler character(88)
) USING orioledb;

CREATE TABLE pgbench_tellers (
	tid integer NOT NULL,
	bid integer,
	tbalance integer,
	filler character(84)
) USING orioledb;

CREATE TABLE pgbench_history
(
	tid integer NOT NULL,
	bid integer NOT NULL,
	aid integer NOT NULL,
	delta integer NOT NULL,
	mtime timestamp NOT NULL,
	filler character(22),
	PRIMARY KEY(bid, mtime, tid, aid, delta)
) USING orioledb;