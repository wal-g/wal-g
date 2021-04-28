# WAL-G
![Docker-tests-status](https://github.com/wal-g/wal-g/workflows/Docker%20tests/badge.svg)
![Unit-tests-status](https://github.com/wal-g/wal-g/workflows/Unit%20tests/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/wal-g/wal-g)](https://goreportcard.com/report/github.com/wal-g/wal-g)

[Full documentation can be found here](https://wal-g.readthedocs.io)

WAL-G is an archival restoration tool for Postgres(beta for MySQL, MariaDB, and MongoDB)

WAL-G is the successor of WAL-E with a number of key differences. WAL-G uses LZ4, LZMA, or Brotli compression, multiple processors, and non-exclusive base backups for Postgres. More information on the design and implementation of WAL-G can be found on the Citus Data blog post ["Introducing WAL-G by Citus: Faster Disaster Recovery for Postgres"](https://www.citusdata.com/blog/2017/08/18/introducing-wal-g-faster-restores-for-postgres/).

Authors
-------

* [Katie Li](https://github.com/katie31)
* [Daniel Farina](https://github.com/fdr)

See also the list of [contributors](CONTRIBUTORS) who participated in this project.

License
-------

This project is licensed under the Apache License, Version 2.0, but the lzo support is licensed under GPL 3.0+. Please refer to the [LICENSE.md](LICENSE.md) file for more details.

Acknowledgments
----------------
WAL-G would not have happened without the support of [Citus Data](https://www.citusdata.com/)

WAL-G came into existence as a result of the collaboration between a summer engineering intern at Citus, Katie Li, and Daniel Farina, the original author of WAL-E, who currently serves as a principal engineer on the Citus Cloud team. Citus Data also has an [open source extension to Postgres](https://github.com/citusdata) that distributes database queries horizontally to deliver scale and performance.

WAL-G development is supported by [Yandex Cloud](https://cloud.yandex.com)

Chat
----
We have a [Slack group](https://postgresteam.slack.com/messages/CA25P48P2) and [Telegram chat](https://t.me/joinchat/C03q9FOwa7GgIIW5CwfjrQ) to discuss WAL-G usage and development. To join PostgreSQL slack use [invite app](https://postgres-slack.herokuapp.com).
