package blob

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"

	//"io/ioutil"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	BlockListTag     = "BlockList"
	BlockLatest      = "Latest"
	BlockCommitted   = "Committed"
	BlockUncommitted = "Uncommitted"
	BlockAll         = "All"
)

type XBlockIn struct {
	ID   string
	Mode string
}

type XBlockListIn struct {
	Blocks []XBlockIn
}

func (b *XBlockIn) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	name := start.Name.Local
	if name != BlockLatest && name != BlockCommitted && name != BlockUncommitted {
		return fmt.Errorf("unexpected block tag: %v", start.Name.Local)
	}
	b.Mode = name
	for {
		t, err := d.Token()
		if err != nil {
			return err
		}
		switch t2 := t.(type) {
		case xml.CharData:
			b.ID = strings.TrimSpace(string(t2))
		case xml.EndElement:
			if t2.Name.Local == name {
				return nil
			}
			return fmt.Errorf("unexpected closing block tag: %v", t2.Name.Local)
		case xml.ProcInst:
		case xml.Comment:
		case xml.Directive:
		default:
			return fmt.Errorf("unexpected token: %v", t)
		}
	}
}

func (b *XBlockIn) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	var err error
	err = e.EncodeToken(xml.StartElement{Name: xml.Name{Local: b.Mode}})
	if err != nil {
		return err
	}
	err = e.EncodeToken(xml.CharData(b.ID))
	if err != nil {
		return err
	}
	err = e.EncodeToken(xml.EndElement{Name: xml.Name{Local: b.Mode}})
	return err
}

func (bl *XBlockListIn) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	if start.Name.Local != BlockListTag {
		return fmt.Errorf("unexpected block list tag: %v", start.Name.Local)
	}
	for {
		t, err := d.Token()
		if err != nil {
			return err
		}
		switch t2 := t.(type) {
		case xml.StartElement:
			var b XBlockIn
			err = d.DecodeElement(&b, &t2)
			if err != nil {
				return err
			}
			bl.Blocks = append(bl.Blocks, b)
		case xml.EndElement:
			if t2.Name.Local == BlockListTag {
				return nil
			}
			return fmt.Errorf("unexpected closing blocklist tag: %v", t2.Name.Local)
		case xml.CharData:
		case xml.ProcInst:
		case xml.Comment:
		case xml.Directive:
		default:
			return fmt.Errorf("unexpected token: %v", t)
		}
	}
}

func (bl *XBlockListIn) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	var err error
	err = e.EncodeToken(xml.StartElement{Name: xml.Name{Local: BlockListTag}})
	if err != nil {
		return err
	}
	for _, b := range bl.Blocks {
		err = e.Encode(b)
		if err != nil {
			return err
		}
	}
	err = e.EncodeToken(xml.EndElement{Name: xml.Name{Local: BlockListTag}})
	return err
}

func ParseBlocklistXML(data []byte) (*XBlockListIn, error) {
	bl := &XBlockListIn{}
	data = utf16utf8(data, binary.LittleEndian)
	d := xml.NewDecoder(bytes.NewBuffer(data))
	d.CharsetReader = func(s string, r io.Reader) (io.Reader, error) {
		return r, nil
	}
	err := d.Decode(bl)
	if err != nil {
		return nil, err
	}
	return bl, nil
}

// 21century, we can't convert charset in golang. nice
func utf16utf8(b []byte, o binary.ByteOrder) []byte {
	utf := make([]uint16, (len(b)+(2-1))/2)
	for i := 0; i+(2-1) < len(b); i += 2 {
		utf[i/2] = o.Uint16(b[i:])
	}
	if len(b)/2 < len(utf) {
		utf[len(utf)-1] = utf8.RuneError
	}
	return []byte(string(utf16.Decode(utf)))
}

type XBlockListOut struct {
	XMLName         xml.Name `xml:"BlockList"`
	CommittedBlocks struct {
		Blocks []XBlockOut
	}
	UncommittedBlocks struct {
		Blocks []XBlockOut
	}
}

type XBlockOut struct {
	XMLName xml.Name `xml:"Block"`
	Name    string
	Size    uint64
}

func SerializeBlocklistXML(bl *XBlockListOut) ([]byte, error) {
	return xml.Marshal(bl)
}
