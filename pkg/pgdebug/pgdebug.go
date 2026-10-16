package pgdebug

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
)

const (
	sslRequestCode  = 80877103
	gsencNegotiate  = 80877104
	protocolVersion = 196608 // 3.0
)

func Serve(l net.Listener, version string) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer c.Close()
			h := &conn{c: c, r: bufio.NewReader(c), version: version}
			h.run()
		}()
	}
}

type conn struct {
	c       net.Conn
	r       *bufio.Reader
	version string
}

type msg struct {
	typ byte
	buf []byte
}

func readMsg(r io.Reader) (msg, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return msg{}, err
	}
	n := binary.BigEndian.Uint32(head[1:]) - 4
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return msg{}, err
	}
	return msg{head[0], buf}, nil
}

func (m msg) str() string { // null-terminated prefixed string
	return strings.TrimRight(string(m.buf), "\x00")
}

func (c *conn) send(bs ...[]byte) error {
	for _, b := range bs {
		if _, err := c.c.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) run() {
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(c.r, lenBuf[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(lenBuf[:]) - 4
		body := make([]byte, n)
		if _, err := io.ReadFull(c.r, body); err != nil {
			return
		}
		code := binary.BigEndian.Uint32(body[:4])
		switch code {
		case sslRequestCode, gsencNegotiate:
			c.c.Write([]byte{'N'})
		case protocolVersion, 196610:
			c.send(authOk(), readyForQuery('I'))
			c.loop()
			return
		default:
			return
		}
	}
}

func (c *conn) loop() {
	for {
		m, err := readMsg(c.r)
		if err != nil {
			return
		}
		switch m.typ {
		case 'Q':
			q := strings.ToLower(strings.TrimSpace(m.str()))
			if q == "show version" {
				c.send(rowDesc("version"), dataRow(c.version), commandComplete("SHOW"), readyForQuery('I'))
			} else {
				c.send(errResp("unknown command"), readyForQuery('I'))
			}
		case 'X':
			return
		}
	}
}

func head(t byte, body []byte) []byte {
	h := make([]byte, 5)
	h[0] = t
	binary.BigEndian.PutUint32(h[1:], uint32(len(body)+4))
	return append(h, body...)
}

func cstr(s string) []byte { return append([]byte(s), 0) }

func i16(v int) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return b
}

func authOk() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 0)
	return head('R', b)
}

func rowDesc(name string) []byte {
	var b []byte
	nf := 1
	b = append(b, i16(nf)...)
	b = append(b, cstr(name)...)
	b = append(b, 0, 0, 0, 0)             // table oid i32
	b = append(b, i16(0)...)              // column attnum
	b = append(b, []byte{25, 0, 0, 0}...) // type oid: text
	b = append(b, i16(-1)...)             // typlen
	b = append(b, 0, 0, 0, 0)             // typmod i32
	b = append(b, i16(0)...)              // format: text
	return head('T', b)
}

func dataRow(vals ...string) []byte {
	var b []byte
	b = append(b, i16(len(vals))...)
	for _, v := range vals {
		l := len(v)
		lb := make([]byte, 4)
		binary.BigEndian.PutUint32(lb, uint32(l))
		b = append(b, lb...)
		b = append(b, []byte(v)...)
	}
	return head('D', b)
}

func commandComplete(tag string) []byte {
	return head('C', cstr(tag))
}

func readyForQuery(status byte) []byte {
	return head('Z', []byte{status})
}

func errResp(msg string) []byte {
	body := append([]byte("SERROR\x00C42601\x00M"), []byte(msg)...)
	body = append(body, 0, 0)
	return head('E', body)
}
