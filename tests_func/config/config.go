package config

var Env = map[string]string{
	"STAGING_DIR":        "staging",
	"ENV_FILE":           "staging/env.file",
	"IMAGES_DIR":         "images",
	"DOCKER_BRIDGE_NAME": "walg-func-test",
	"DOCKER_IP4_SUBNET":  "10.%s.0/24",
	"DOCKER_IP6_SUBNET":  "fd00:dead:beef:%s::/96",

	"COMPOSE_FILE":       "docker-compose.yml",
	"TEST_ID":            "13",
	"TEST_CLEANUP_DELAY": "60",

	"WALG_S3_PREFIX":   "s3://dbaas/mongodb-backup/test_uuid/test_mongodb",
	"WALG_CLIENT_PATH": "/usr/bin/wal-g",
	"WALG_CONF_DIR":    "/etc/wal-g",
	"WALG_CONF_PATH":   "/etc/wal-g/wal-g.json",

	"MONGO_BUILD_PATH":     ".",
	"MONGO_ADMIN_USERNAME": "admin",
	"MONGO_ADMIN_PASSWORD": "password",
	"MONGO_ADMIN_DB_NAME":  "admin",
	"MONGO_EXPOSE_MONGOD":  "27018",
	"MONGO_EXPOSE_SSH":     "22",

	"S3_HOST":          "minio01",
	"S3_PORT":          "9000",
	"S3_BUCKET":        "dbaas",
	"S3_ACCESS_KEY":    "S3_ACCESS_KEY",
	"S3_SECRET_KEY":    "S3_SECRET_KEY",

	"BACKUP_BASE_TAG":  "walg-func-test-base",
	"BACKUP_BASE_PATH": "staging/images/base",
}
