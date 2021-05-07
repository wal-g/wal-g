# WAL-G for Windows

Building Windows executable on Linux

## See here for available GOOS and GOARCH values.
-----------

Go version >= 1.5
Since Go version 1.5 cross-compiling of pure Go executables has become very easy.

* export GOOS=windows
* export GOARCH=amd64

* make install
* make deps
* make sqlserver_build

* mv main/sqlserver/wal-g{,.exe}

Development on Windows
-----------
### Installing
To develop wal-g on windows you can use WSL:

*  [Install WSL](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
* Install golang on your WSL
* Install wal-g using [README](https://github.com/wal-g/wal-g/blob/master/README.md)
### Usage
* Now you can open wal-g project in Windows. Path for your WSL (if it's ubuntu 16.04 lts) is 
`C:/Users/<WindowsUserName>/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu16.04onWindows_79rhkp1fndgsc/LocalState/rootfs/home/<WSLUserName>`

