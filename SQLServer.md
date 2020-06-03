wal-g proxy emulates azure blob storage on local host
It allows you to use BACKUP TO URL syntax, and store backup in any storage

SQLServer requires URL to look like https://fqdn.com/folder/...
So you need to:
* create put fake DNS record '127.0.0.1 fqdn.com' in C:\Windows\System32\Drivers\etc\hosts
* create and import self-signed certificate for fqdn.com

Your wal-g.json config for SQLServer than may look like
    {
    "WALG_FILE_PREFIX": "C:/backup",
    "SQLSERVER_BLOB_CERT_FILE": "C:/Path/To/cert.pem",
    "SQLSERVER_BLOB_KEY_FILE":  "C:/Path/To/key.pem",
    "SQLSERVER_BLOB_ENDPOINT":  "fqdn.com:443"
    }

Of course, you may use any wal-g storage instead of FILE
