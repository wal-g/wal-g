package xbstream

/*
`xxx.delta` files contains list of following blocks:
```
  - Header - page_size bytes (page_size - from '.meta' file)
    (4 bytes) 'xtra' or 'XTRA' (for last block)
    (N * 4 bytes) N * page_number  - list of page_number (up to page_size/4 entries OR  0xFFFFFFFF-terminated-list)
  - Body
    N * <page content>

```
*/

const (
	DELTA_STREAM_MAGIC      = uint32(0x78747261) // 'xtra'
	DELTA_STREAM_MAGIC_LAST = uint32(0x58545241) // 'XTRA'
	PAGE_LIST_TERMINATOR    = uint32(0xFFFFFFFF)
)

var (
	DELTA_STREAM_MAGIC_BYTES      = []byte("xtra")
	DELTA_STREAM_MAGIC_LAST_BYTES = []byte("XTRA")
)
