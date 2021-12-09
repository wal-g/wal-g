## Storage tools (danger zone)
`wal-g st` command series allows interacting with the configured storage. Be aware that these commands can do potentially harmful operations and make sure that you know what you're doing.

### ``ls``
Prints listing of the objects in the provided storage folder.

``wal-g st ls`` get listing with all objects and folders in the configured storage.

``wal-g st ls -r`` get recursive listing with all objects in the configured storage.

``wal-g st ls some_folder/some_subfolder`` get listing with all objects in the provided storage path.

### ``get``
Download the specified storage object. By default, the command will try to apply the decompression and decryption (if configured).

Flags:
1. Add `--no-decompress` to download the remote object without decompression
2. Add `--no-decrypt` to download the remote object without decryption

Examples:

``wal-g st get path/to/remote_file path/to/local_file`` download the file from storage.

``wal-g st get path/to/remote_file path/to/local_file --no-decrypt`` download the file from storage without decryption.

### ``cat``
Show the specified storage object to STDOUT. 
By default, the command will NOT try to decompress and decrypt it.
Useful for getting sentinels and other meta-information files.

Flags:
1. Add `--decompress` to decompress source file
2. Add `--decrypt` to decrypt source file

Examples:

``wal-g st cat path/to/remote_file.json`` show `remote_file.json`

### ``rm``
Remove the specified storage object.

Example:

``wal-g st rm path/to/remote_file`` remove the file from storage.

### ``put``
Upload the specified file to the storage. By default, the command will try to apply the compression and encryption (if configured).

Flags:
1. Add `--no-compress` to upload the object without compression
2. Add `--no-encrypt` to upload the object without encryption

Example:

``wal-g st put path/to/local_file path/to/remote_file`` upload the local file to the storage.
