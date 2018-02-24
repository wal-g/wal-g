PKG  := github.com/wal-g/wal-g
CMD_FILES = $(wildcard cmd/wal-g/*.go)
PKG_FILES = $(wildcard *.go)

.PHONY : fmt test install all clean build

test: build
	go list ./... | grep -v 'vendor/' | xargs go vet
	go test -v

fmt: $(CMD_FILES) $(PKG_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES)

all: build

install:
	(cd cmd/wal-g && go install)

clean:
	rm -r extracted compressed $(wildcard data*)
	go clean
	(cd cmd/wal-g && go clean)
	rm -rf bin

build: bin/linux-amd64 bin/alpine-amd64

bin/linux-amd64: $(CMD_FILES) $(PKG_FILES)
	(GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/linux-amd64/wal-g cmd/wal-g/*.go)

bin/alpine-amd64: $(CMD_FILES) $(PKG_FILES)
	docker run                                                              \
	    --rm                                                                \
	    -u $$(id -u):$$(id -g)                                              \
	    -v /tmp:/.cache                                                     \
	    -v "$$(pwd):/go/src/$(PKG)"                                         \
	    -w /go/src/$(PKG)                                                   \
	    -e GOOS=linux                                                       \
	    -e GOARCH=amd64                                                     \
	    -e CGO_ENABLED=0                                                    \
	    golang:1.10.0-alpine                                                \
	    go build -o bin/alpine-amd64/wal-g cmd/wal-g/*.go
