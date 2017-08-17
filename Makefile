CMD_FILES = $(wildcard cmd/wal-g/*.go)
PKG_FILES = $(wildcard *.go)

.PHONY : test install all clean

all: cmd/wal-g/wal-g	

test:
	go test -v

install:
	(cd cmd/wal-g && go install)

clean:
	rm -r extracted compressed $(wildcard data*)
	go clean
	(cd cmd/wal-g && go clean)

cmd/wal-g/wal-g: $(CMD_FILES) $(PKG_FILES)
	(cd cmd/wal-g && go build)
