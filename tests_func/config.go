package functests

var Env = map[string]string{
	"IMAGES_DIR":         "images",
	"DOCKER_BRIDGE_NAME": "walg-func-test",
	"DOCKER_IP4_SUBNET":  "10.%s.0/24",
	"DOCKER_IP6_SUBNET":  "fd00:dead:beef:%s::/96",

	"COMPOSE_FILE_SUFFIX": "-docker-compose.yml",
	"TEST_ID":             "13",
	"TEST_CLEANUP_DELAY":  "60",

	"WALG_S3_PREFIX":   "s3://dbaas/DBNAME-backup/test_uuid/test_DBNAME",
	"WALG_CLIENT_PATH": "/usr/bin/wal-g",
	"WALG_CONF_DIR":    "/etc/wal-g",
	"WALG_CONF_PATH":   "/etc/wal-g/wal-g.json",

	"S3_HOST":       "minio01",
	"S3_PORT":       "9000",
	"S3_BUCKET":     "dbaas",
	"S3_ACCESS_KEY": "S3_ACCESS_KEY",
	"S3_SECRET_KEY": "S3_SECRET_KEY",

	"EXPOSE_SSH_PORT": "22",

	"BACKUP_BASE_TAG":  "walg-func-test-base",
	"BACKUP_BASE_PATH": "staging/images/base",

	// Mongodb specific
	"MONGO_BUILD_PATH":     ".",
	"MONGO_ADMIN_USERNAME": "admin",
	"MONGO_ADMIN_PASSWORD": "password",
	"MONGO_ADMIN_DB_NAME":  "admin",
	"MONGO_EXPOSE_MONGOD":  "27018",

	// Redis specific
	"REDIS_EXPOSE_PORT": "6379",
	"REDIS_PASSWORD":    "password",
}
