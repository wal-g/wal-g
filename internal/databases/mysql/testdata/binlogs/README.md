# MySQL binlog fixtures

- MySQL server version: `8.4.11-12`.
- `server_uuid`: `da187401-e4f3-11f1-9478-92bd74c99a74`.

Each file contains one transaction: `mysql-bin.000001` through
`mysql-bin.000006` contain GTIDs `<server_uuid>:1` through `<server_uuid>:6`,
respectively. The first file has an empty `PREVIOUS_GTIDS` set; each subsequent
file includes all preceding transactions in its `PREVIOUS_GTIDS` header.

Used to test binlog filtering and GTID metadata parsing with real binlog files.
