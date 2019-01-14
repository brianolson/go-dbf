package dbf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"unicode"
)

// DBase database file format, just enough to read Census shapefile bundles.
// http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
// https://www.clicketyclick.dk/databases/xbase/format/dbf.html
type Dbf struct {
	Version        byte
	Year           int
	Month          int
	Day            int
	NumRecords     uint32
	NumHeaderBytes uint16
	NumRecordBytes uint16
	Incomplete     byte
	Encrypted      byte
	Mdx            byte
	Language       byte
	DriverName     string
	Fields         []DbfField

	recordLength int
	recordBuffer []byte

	reader io.ReadCloser
}
type DbfFieldType uint8
type DbfField struct {
	Name   string
	Type   DbfFieldType
	Length uint8
	Count  uint8

	// StartPos is the calulated (not read from file) position within fixed size record row
	StartPos int

	// reference to the db containing this field, so we can read a value out of the current row buffer
	d *Dbf
}

const (
	DbfFieldNumeric DbfFieldType = DbfFieldType('N')
	DbfFieldChar    DbfFieldType = DbfFieldType('C')
)

var BadHeaderLength error = errors.New("Bad dbf header length")

func dbtrim(x string) string {
	return strings.TrimFunc(x, func(r rune) bool {
		return r == rune(0) || unicode.IsSpace(r)
	})
}

// Parse loads the next chunk of DBF header into this field record
func (h *DbfField) Parse(data []byte) error {
	if len(data) == 32 {
		h.Name = dbtrim(string(data[0:11]))
		h.Type = DbfFieldType(data[11])
		h.Length = data[16]
		h.Count = data[17]
	} else if len(data) == 48 {
		h.Name = dbtrim(string(data[0:32]))
		h.Type = DbfFieldType(data[32])
		h.Length = data[33]
		h.Count = data[34]
	} else {
		return BadHeaderLength
	}
	return nil
}

// GoString is the debug string describing the field header information
func (h *DbfField) GoString() string {
	return fmt.Sprintf("(%#v %c l=%d c=%d)", h.Name, rune(h.Type), h.Length, h.Count)
}
func (h *DbfField) String() string {
	return h.GoString()
}

// StringValue is the value of this field for the current row.
func (h *DbfField) StringValue() string {
	return strings.TrimSpace(string(h.d.recordBuffer[h.StartPos : h.StartPos+int(h.Length)]))
}

func (h *DbfField) Int64() (i int64, err error) {
	return strconv.ParseInt(h.StringValue(), 10, 64)
}

// NewDbf reads the header immediately and may return (nil, error)
func NewDbf(reader io.ReadCloser) (d *Dbf, err error) {
	d = &Dbf{reader: reader}
	err = d.readHeader()
	if err != nil {
		d = nil
	}
	return
}

func (d *Dbf) readHeader() error {
	var scratch [32]byte
	_, err := io.ReadFull(d.reader, scratch[:])
	if err != nil {
		return err
	}
	d.Version = scratch[0]
	d.Year = int(uint8(scratch[1])) + 1900
	d.Month = int(scratch[2])
	d.Day = int(scratch[3])
	d.NumRecords = binary.LittleEndian.Uint32(scratch[4:8])
	d.NumHeaderBytes = binary.LittleEndian.Uint16(scratch[8:10])
	d.NumRecordBytes = binary.LittleEndian.Uint16(scratch[10:12])
	d.Incomplete = scratch[14]
	d.Encrypted = scratch[15]
	d.Mdx = scratch[28]
	d.Language = scratch[29]
	var headerSize int
	if (d.Version & 0x07) == 4 {
		namebuf := make([]byte, 32)
		_, err = io.ReadFull(d.reader, namebuf)
		if err != nil {
			return err
		}
		d.DriverName = strings.TrimSpace(string(namebuf))
		// skip 4 bytes
		_, err = io.ReadFull(d.reader, scratch[0:4])
		if err != nil {
			return err
		}
		headerSize = 48
	} else if (d.Version & 0x07) == 3 {
		headerSize = 32
	} else {
		return fmt.Errorf("Unkown dbf version %x", d.Version)
	}
	hbuf := make([]byte, headerSize)
	_, err = io.ReadFull(d.reader, hbuf[0:1])
	if err != nil {
		return err
	}
	startPos := 0
	for hbuf[0] != 0x0d {
		_, err = io.ReadFull(d.reader, hbuf[1:])
		if err != nil {
			return err
		}
		var field DbfField
		err = field.Parse(hbuf)
		if err != nil {
			return err
		}
		field.StartPos = startPos
		field.d = d
		startPos += int(field.Length)
		d.Fields = append(d.Fields, field)
		_, err = io.ReadFull(d.reader, hbuf[0:1])
		if err != nil {
			return err
		}
	}
	d.recordLength = startPos
	if d.recordLength+1 != int(d.NumRecordBytes) {
		log.Print("NumRecordBytes=", d.NumRecordBytes, " calculated record length=", d.recordLength)
	}
	d.recordBuffer = make([]byte, d.recordLength)

	return nil
}

// Next returns nil error when ok, io.EOF as apporpriate, or other underlying errors.
func (d *Dbf) Next() error {
	if d.reader == nil {
		return io.EOF
	}
	actual, err := d.reader.Read(d.recordBuffer[0:1])
	if err != nil {
		return err
	} else if actual != 1 {
		d.Close()
		return io.EOF
	}
	if d.recordBuffer[0] == 0x1a {
		d.Close()
		return io.EOF
	}
	_, err = io.ReadFull(d.reader, d.recordBuffer)
	return err
}

func (d *Dbf) Close() error {
	if d.reader != nil {
		err := d.reader.Close()
		d.reader = nil
		return err
	}
	return nil
}
