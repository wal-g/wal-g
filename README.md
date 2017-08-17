# WAL-G

WAL-G 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Installing
--------------

A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

### Configuration
-----------------
To connect to Amazon S3, WAL-G requires that these variables be set:

* `WALE_S3_PREFIX` (eg. `s3://bucket/path/to/folder`)
* `AWS_REGION`(eg. `us-west-2`)
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_SECURITY_TOKEN`

Also note that WAL-G uses this environment variable to connect to Postgres:

* `PGHOST`

**Optional:**

Concurrency values can be configured using:

* `WALG_MAX_CONCURRENCY`

To configure how many goroutines to use during extraction, set this variable. By default, WAL-G uses the minimum of the number of files to extract and 10.

* `WALG_UPLOAD_CONCURRENCY`

To configure how many concurrency streams to use during backup uploading, set this variable. By default, WAL-G uses 10 streams.



## Usage
-----------
WAL-G currently supports these commands:

<br/>

* ``backup-fetch``

When fetching base backups, the user should pass in the name of the backup and a path to a directory to extract to. If this directory does not exist, WAL-G will create it and any dependent subdirectories. 

```
wal-g backup-fetch ~/extract/to/here example-backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch ~/extract/to/here LATEST
```
<br/>

* ``backup-push``



Upload a base backup to S3 using:

```
wal-g backup-push /backup/directory/path
```
<br/>

* ``wal-fetch``

<br/>

* ``wal-push``



### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* [Daniel Farina](https://github.com/fdr)
* [Katie Li](https://github.com/katie31)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. 

Please refer to the [LICENSE.md](LICENSE.md) file for more details.

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc
