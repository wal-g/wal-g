package blob

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"

	"golang.org/x/net/html/charset"

	"strings"
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
	d := xml.NewDecoder(bytes.NewBuffer(data))
	d.CharsetReader = func(s string, r io.Reader) (io.Reader, error) {
		if s == "utf-16" {
			return charset.NewReader(r, "charset=utf-16")
		}
		return r, nil
	}
	err := d.Decode(bl)
	if err != nil {
		return nil, err
	}
	return bl, nil
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
