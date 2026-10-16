package pgdebug

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestShowVersion(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	go Serve(l, "test-1.0")
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// SSLRequest -> expect 'N'
	tt := uint32(80877103)
	c.Write([]byte{0, 0, 0, 8, byte(tt >> 24), byte(tt >> 16), byte(tt >> 8), byte(tt)})
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c, buf); err != nil || buf[0] != 'N' {
		t.Fatalf("expected N, got %q err %v", buf[0], err)
	}

	startup := append([]byte{0, 0, 0, 19}, 0, 3, 0, 0)
	startup = append(startup, []byte("user\x00test\x00\x00")...)
	c.Write(startup)
	c.Write([]byte("Q\x00\x00\x00\x11show version\x00"))

	var resp []byte
	deadline := time.Now().Add(5 * time.Second)
	c.SetReadDeadline(deadline)
	tmp := make([]byte, 4096)
	for time.Now().Before(deadline) && !strings.Contains(string(resp), "SHOW") {
		n, err := c.Read(tmp)
		resp = append(resp, tmp[:n]...)
		if err != nil {
			break
		}
	}
	s := string(resp)
	for _, want := range []string{"version", "test-1.0", "SHOW"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in %q", want, s)
		}
	}
}
