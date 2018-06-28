CMD_FILES = $(wildcard cmd/wal-g/*.go)
PKG_FILES = $(wildcard *.go)

.PHONY : fmt test install all clean

ifdef GOTAGS
override GOTAGS := -tags $(GOTAGS)
endif

test: cmd/wal-g/wal-g
	go list ./... | grep -v 'vendor/' | xargs go vet
	go test $(GOTAGS) -v

fmt: $(CMD_FILES) $(PKG_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES)

all: cmd/wal-g/wal-g

install:
	(cd cmd/wal-g && go install)

clean:
	rm -rf extracted compressed $(wildcard data*)
	go clean
	(cd cmd/wal-g && go clean)

cmd/wal-g/wal-g: $(CMD_FILES) $(PKG_FILES)
	(cd cmd/wal-g && go build $(GOTAGS) -ldflags "-X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X main.GitRevision=`git rev-parse --short HEAD` -X main.WalgVersion=`git tag -l --points-at HEAD`")
