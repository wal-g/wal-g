package config

var Env = map[string]string{
	"STAGING_DIR":        "staging",
	"ENV_FILE":           "staging/env.file",
	"IMAGES_DIR":         "images",
	"DOCKER_BRIDGE_NAME": "dbaas",
	"DOCKER_IP4_SUBNET":  "10.%s.0/24",
	"DOCKER_IP6_SUBNET":  "fd00:dead:beef:%s::/96",

	"COMPOSE_FILE": "./staging/docker-compose.yml",

	"MONGO_HOST_01_WORKER": "mongodb01",
	"MONGO_HOST_02_WORKER": "mongodb02",
	"TEST_ID":              "13",
	"TEST_CLEANUP_DELAY":   "60",

	"WALG_S3_PREFIX":   "s3://dbaas/mongodb-backup/test_uuid/test_mongodb",
	"WALG_CLIENT_PATH": "/usr/bin/wal-g",
	"WALG_CONF_PATH":   "/home/.walg.json",

	"S3_FAKE_HOST": "minio",
	"S3_FAKE_PORT": "9000",
	"S3_WORKER":    "minio01",
	"S3_BUCKET":    "dbaas",

	"MONGO_BUILD_PATH":     "..",
	"MONGO_ADMIN_USERNAME": "admin",
	"MONGO_ADMIN_PASSWORD": "password",
	"MONGO_ADMIN_DB_NAME":  "admin",
	"MONGO_ADMIN_ROLES":    "root",
	"MONGO_EXPOSE_MONGOD":  "27018",
	"MONGO_EXPOSE_SSH":     "22",

	"MINIO_BUILD_PATH":  "staging/images/minio",
	"MINIO_EXPOSE_HTTP": "9000",

	"MONGODB_BACKUP_BASE_TAG":  "mongodb-backup-base",
	"MONGODB_BACKUP_BASE_PATH": "staging/images/base",
}
