package mssql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/denisenkom/go-mssqldb/internal/cp"
	"github.com/denisenkom/go-mssqldb/internal/mstype"
)

const _PLP_NULL = 0xFFFFFFFFFFFFFFFF
const _UNKNOWN_PLP_LEN = 0xFFFFFFFFFFFFFFFE
const _PLP_TERMINATOR = 0x00000000

// TYPE_INFO rule
// http://msdn.microsoft.com/en-us/library/dd358284.aspx
type typeInfo struct {
	TypeID    mstype.ID
	Size      int
	Scale     uint8
	Prec      uint8
	Buffer    []byte
	Collation cp.Collation
	UdtInfo   udtInfo
	XmlInfo   xmlInfo
	Reader    func(ti *typeInfo, r *tdsBuffer) (res interface{})
	Writer    func(w io.Writer, ti typeInfo, buf []byte) (err error)
}

// Common Language Runtime (CLR) Instances
// http://msdn.microsoft.com/en-us/library/dd357962.aspx
type udtInfo struct {
	//MaxByteSize         uint32
	DBName                string
	SchemaName            string
	TypeName              string
	AssemblyQualifiedName string
}

// XML Values
// http://msdn.microsoft.com/en-us/library/dd304764.aspx
type xmlInfo struct {
	SchemaPresent       uint8
	DBName              string
	OwningSchema        string
	XmlSchemaCollection string
}

func readTypeInfo(r *tdsBuffer) (res typeInfo) {
	res.TypeID = mstype.ID(r.byte())
	switch res.TypeID {
	case mstype.Null, mstype.Int1, mstype.Bit, mstype.Int2, mstype.Int4, mstype.DateTim4,
		mstype.Flt4, mstype.Money, mstype.DateTime, mstype.Flt8, mstype.Money4, mstype.Int8:
		// those are fixed length types
		switch res.TypeID {
		case mstype.Null:
			res.Size = 0
		case mstype.Int1, mstype.Bit:
			res.Size = 1
		case mstype.Int2:
			res.Size = 2
		case mstype.Int4, mstype.DateTim4, mstype.Flt4, mstype.Money4:
			res.Size = 4
		case mstype.Money, mstype.DateTime, mstype.Flt8, mstype.Int8:
			res.Size = 8
		}
		res.Reader = readFixedType
		res.Buffer = make([]byte, res.Size)
	default: // all others are VARLENTYPE
		readVarLen(&res, r)
	}
	return
}

func writeTypeInfo(w io.Writer, ti *typeInfo) (err error) {
	err = binary.Write(w, binary.LittleEndian, ti.TypeID)
	if err != nil {
		return
	}
	switch ti.TypeID {
	case mstype.Null, mstype.Int1, mstype.Bit, mstype.Int2, mstype.Int4, mstype.DateTim4,
		mstype.Flt4, mstype.Money, mstype.DateTime, mstype.Flt8, mstype.Money4, mstype.Int8:
		// those are fixed length
		ti.Writer = writeFixedType
	default: // all others are VARLENTYPE
		err = writeVarLen(w, ti)
		if err != nil {
			return
		}
	}
	return
}

func writeFixedType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	_, err = w.Write(buf)
	return
}

func writeVarLen(w io.Writer, ti *typeInfo) (err error) {
	switch ti.TypeID {
	case mstype.DateN:
		ti.Writer = writeByteLenType
	case mstype.TimeN, mstype.DateTime2N, mstype.DateTimeOffsetN:
		if err = binary.Write(w, binary.LittleEndian, ti.Scale); err != nil {
			return
		}
		ti.Writer = writeByteLenType
	case mstype.IntN, mstype.Decimal, mstype.Numeric,
		mstype.BitN, mstype.DecimalN, mstype.NumericN, mstype.FltN,
		mstype.MoneyN, mstype.DateTimeN, mstype.Char,
		mstype.VarChar, mstype.Binary, mstype.VarBinary:

		// byle len types
		if ti.Size > 0xff {
			panic("Invalid size for BYLELEN_TYPE")
		}
		if err = binary.Write(w, binary.LittleEndian, uint8(ti.Size)); err != nil {
			return
		}
		switch ti.TypeID {
		case mstype.Decimal, mstype.Numeric, mstype.DecimalN, mstype.NumericN:
			err = binary.Write(w, binary.LittleEndian, ti.Prec)
			if err != nil {
				return
			}
			err = binary.Write(w, binary.LittleEndian, ti.Scale)
			if err != nil {
				return
			}
		}
		ti.Writer = writeByteLenType
	case mstype.Guid:
		if !(ti.Size == 0x10 || ti.Size == 0x00) {
			panic("Invalid size for BYLELEN_TYPE")
		}
		if err = binary.Write(w, binary.LittleEndian, uint8(ti.Size)); err != nil {
			return
		}
		ti.Writer = writeByteLenType
	case mstype.BigVarBin, mstype.BigVarChar, mstype.BigBinary, mstype.BigChar,
		mstype.NVarChar, mstype.NChar, mstype.Xml, mstype.Udt:
		// short len types
		if ti.Size > 8000 || ti.Size == 0 {
			if err = binary.Write(w, binary.LittleEndian, uint16(0xffff)); err != nil {
				return
			}
			ti.Writer = writePLPType
		} else {
			if err = binary.Write(w, binary.LittleEndian, uint16(ti.Size)); err != nil {
				return
			}
			ti.Writer = writeShortLenType
		}
		switch ti.TypeID {
		case mstype.BigVarChar, mstype.BigChar, mstype.NVarChar, mstype.NChar:
			if err = writeCollation(w, ti.Collation); err != nil {
				return
			}
		case mstype.Xml:
			if err = binary.Write(w, binary.LittleEndian, ti.XmlInfo.SchemaPresent); err != nil {
				return
			}
		}
	case mstype.Text, mstype.Image, mstype.NText, mstype.Variant:
		// LONGLEN_TYPE
		if err = binary.Write(w, binary.LittleEndian, uint32(ti.Size)); err != nil {
			return
		}
		if err = writeCollation(w, ti.Collation); err != nil {
			return
		}
		ti.Writer = writeLongLenType
	default:
		panic("Invalid type")
	}
	return
}

// http://msdn.microsoft.com/en-us/library/ee780895.aspx
func decodeDateTim4(buf []byte) time.Time {
	days := binary.LittleEndian.Uint16(buf)
	mins := binary.LittleEndian.Uint16(buf[2:])
	return time.Date(1900, 1, 1+int(days),
		0, int(mins), 0, 0, time.UTC)
}

func encodeDateTim4(val time.Time) (buf []byte) {
	buf = make([]byte, 4)

	ref := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	dur := val.Sub(ref)
	days := dur / (24 * time.Hour)
	mins := val.Hour()*60 + val.Minute()
	if days < 0 {
		days = 0
		mins = 0
	}

	binary.LittleEndian.PutUint16(buf[:2], uint16(days))
	binary.LittleEndian.PutUint16(buf[2:], uint16(mins))
	return
}

// encodes datetime value
// type identifier is typeDateTimeN
func encodeDateTime(t time.Time) (res []byte) {
	// base date in days since Jan 1st 1900
	basedays := gregorianDays(1900, 1)
	// days since Jan 1st 1900 (same TZ as t)
	days := gregorianDays(t.Year(), t.YearDay()) - basedays
	tm := 300*(t.Second() + t.Minute()*60 + t.Hour()*60*60) + t.Nanosecond()*300/1e9
	// minimum and maximum possible
	mindays := gregorianDays(1753, 1) - basedays
	maxdays := gregorianDays(9999, 365) - basedays
	if days < mindays {
		days = mindays
		tm = 0
	}
	if days > maxdays {
		days = maxdays
		tm = (23*60*60 + 59*60 + 59)*300 + 299
	}
	res = make([]byte, 8)
	binary.LittleEndian.PutUint32(res[0:4], uint32(days))
	binary.LittleEndian.PutUint32(res[4:8], uint32(tm))
	return
}

func decodeDateTime(buf []byte) time.Time {
	days := int32(binary.LittleEndian.Uint32(buf))
	tm := binary.LittleEndian.Uint32(buf[4:])
	ns := int(math.Trunc(float64(tm%300)/0.3+0.5)) * 1000000
	secs := int(tm / 300)
	return time.Date(1900, 1, 1+int(days),
		0, 0, secs, ns, time.UTC)
}

func readFixedType(ti *typeInfo, r *tdsBuffer) interface{} {
	r.ReadFull(ti.Buffer)
	buf := ti.Buffer
	switch ti.TypeID {
	case mstype.Null:
		return nil
	case mstype.Int1:
		return int64(buf[0])
	case mstype.Bit:
		return buf[0] != 0
	case mstype.Int2:
		return int64(int16(binary.LittleEndian.Uint16(buf)))
	case mstype.Int4:
		return int64(int32(binary.LittleEndian.Uint32(buf)))
	case mstype.DateTim4:
		return decodeDateTim4(buf)
	case mstype.Flt4:
		return math.Float32frombits(binary.LittleEndian.Uint32(buf))
	case mstype.Money4:
		return decodeMoney4(buf)
	case mstype.Money:
		return decodeMoney(buf)
	case mstype.DateTime:
		return decodeDateTime(buf)
	case mstype.Flt8:
		return math.Float64frombits(binary.LittleEndian.Uint64(buf))
	case mstype.Int8:
		return int64(binary.LittleEndian.Uint64(buf))
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

func readByteLenType(ti *typeInfo, r *tdsBuffer) interface{} {
	size := r.byte()
	if size == 0 {
		return nil
	}
	r.ReadFull(ti.Buffer[:size])
	buf := ti.Buffer[:size]
	switch ti.TypeID {
	case mstype.DateN:
		if len(buf) != 3 {
			badStreamPanicf("Invalid size for DATENTYPE")
		}
		return decodeDate(buf)
	case mstype.TimeN:
		return decodeTime(ti.Scale, buf)
	case mstype.DateTime2N:
		return decodeDateTime2(ti.Scale, buf)
	case mstype.DateTimeOffsetN:
		return decodeDateTimeOffset(ti.Scale, buf)
	case mstype.Guid:
		return decodeGuid(buf)
	case mstype.IntN:
		switch len(buf) {
		case 1:
			return int64(buf[0])
		case 2:
			return int64(int16((binary.LittleEndian.Uint16(buf))))
		case 4:
			return int64(int32(binary.LittleEndian.Uint32(buf)))
		case 8:
			return int64(binary.LittleEndian.Uint64(buf))
		default:
			badStreamPanicf("Invalid size for INTNTYPE")
		}
	case mstype.Decimal, mstype.Numeric, mstype.DecimalN, mstype.NumericN:
		return decodeDecimal(ti.Prec, ti.Scale, buf)
	case mstype.BitN:
		if len(buf) != 1 {
			badStreamPanicf("Invalid size for BITNTYPE")
		}
		return buf[0] != 0
	case mstype.FltN:
		switch len(buf) {
		case 4:
			return float64(math.Float32frombits(binary.LittleEndian.Uint32(buf)))
		case 8:
			return math.Float64frombits(binary.LittleEndian.Uint64(buf))
		default:
			badStreamPanicf("Invalid size for FLTNTYPE")
		}
	case mstype.MoneyN:
		switch len(buf) {
		case 4:
			return decodeMoney4(buf)
		case 8:
			return decodeMoney(buf)
		default:
			badStreamPanicf("Invalid size for MONEYNTYPE")
		}
	case mstype.DateTim4:
		return decodeDateTim4(buf)
	case mstype.DateTime:
		return decodeDateTime(buf)
	case mstype.DateTimeN:
		switch len(buf) {
		case 4:
			return decodeDateTim4(buf)
		case 8:
			return decodeDateTime(buf)
		default:
			badStreamPanicf("Invalid size for DATETIMENTYPE")
		}
	case mstype.Char, mstype.VarChar:
		return decodeChar(ti.Collation, buf)
	case mstype.Binary, mstype.VarBinary:
		// a copy, because the backing array for ti.Buffer is reused
		// and can be overwritten by the next row while this row waits
		// in a buffered chan
		cpy := make([]byte, len(buf))
		copy(cpy, buf)
		return cpy
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

func writeByteLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if ti.Size > 0xff {
		panic("Invalid size for BYTELEN_TYPE")
	}
	err = binary.Write(w, binary.LittleEndian, uint8(ti.Size))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readShortLenType(ti *typeInfo, r *tdsBuffer) interface{} {
	size := r.uint16()
	if size == 0xffff {
		return nil
	}
	r.ReadFull(ti.Buffer[:size])
	buf := ti.Buffer[:size]
	switch ti.TypeID {
	case mstype.BigVarChar, mstype.BigChar:
		return decodeChar(ti.Collation, buf)
	case mstype.BigVarBin, mstype.BigBinary:
		// a copy, because the backing array for ti.Buffer is reused
		// and can be overwritten by the next row while this row waits
		// in a buffered chan
		cpy := make([]byte, len(buf))
		copy(cpy, buf)
		return cpy
	case mstype.NVarChar, mstype.NChar:
		return decodeNChar(buf)
	case mstype.Udt:
		return decodeUdt(*ti, buf)
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

func writeShortLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if buf == nil {
		err = binary.Write(w, binary.LittleEndian, uint16(0xffff))
		return
	}
	if ti.Size > 0xfffe {
		panic("Invalid size for USHORTLEN_TYPE")
	}
	err = binary.Write(w, binary.LittleEndian, uint16(ti.Size))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readLongLenType(ti *typeInfo, r *tdsBuffer) interface{} {
	// information about this format can be found here:
	// http://msdn.microsoft.com/en-us/library/dd304783.aspx
	// and here:
	// http://msdn.microsoft.com/en-us/library/dd357254.aspx
	textptrsize := int(r.byte())
	if textptrsize == 0 {
		return nil
	}
	textptr := make([]byte, textptrsize)
	r.ReadFull(textptr)
	timestamp := r.uint64()
	_ = timestamp // ignore timestamp
	size := r.int32()
	if size == -1 {
		return nil
	}
	buf := make([]byte, size)
	r.ReadFull(buf)
	switch ti.TypeID {
	case mstype.Text:
		return decodeChar(ti.Collation, buf)
	case mstype.Image:
		return buf
	case mstype.NText:
		return decodeNChar(buf)
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}
func writeLongLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	//textptr
	err = binary.Write(w, binary.LittleEndian, byte(0x10))
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, uint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, uint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return
	}
	//timestamp?
	err = binary.Write(w, binary.LittleEndian, uint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return
	}

	err = binary.Write(w, binary.LittleEndian, uint32(ti.Size))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readCollation(r *tdsBuffer) (res cp.Collation) {
	res.LcidAndFlags = r.uint32()
	res.SortId = r.byte()
	return
}

func writeCollation(w io.Writer, col cp.Collation) (err error) {
	if err = binary.Write(w, binary.LittleEndian, col.LcidAndFlags); err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, col.SortId)
	return
}

// reads variant value
// http://msdn.microsoft.com/en-us/library/dd303302.aspx
func readVariantType(ti *typeInfo, r *tdsBuffer) interface{} {
	size := r.int32()
	if size == 0 {
		return nil
	}
	vartype := mstype.ID(r.byte())
	propbytes := int32(r.byte())
	switch vartype {
	case mstype.Guid:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return buf
	case mstype.Bit:
		return r.byte() != 0
	case mstype.Int1:
		return int64(r.byte())
	case mstype.Int2:
		return int64(int16(r.uint16()))
	case mstype.Int4:
		return int64(r.int32())
	case mstype.Int8:
		return int64(r.uint64())
	case mstype.DateTime:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTime(buf)
	case mstype.DateTim4:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTim4(buf)
	case mstype.Flt4:
		return float64(math.Float32frombits(r.uint32()))
	case mstype.Flt8:
		return math.Float64frombits(r.uint64())
	case mstype.Money4:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeMoney4(buf)
	case mstype.Money:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeMoney(buf)
	case mstype.DateN:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDate(buf)
	case mstype.TimeN:
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeTime(scale, buf)
	case mstype.DateTime2N:
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTime2(scale, buf)
	case mstype.DateTimeOffsetN:
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTimeOffset(scale, buf)
	case mstype.BigVarBin, mstype.BigBinary:
		r.uint16() // max length, ignoring
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return buf
	case mstype.DecimalN, mstype.NumericN:
		prec := r.byte()
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDecimal(prec, scale, buf)
	case mstype.BigVarChar, mstype.BigChar:
		col := readCollation(r)
		r.uint16() // max length, ignoring
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeChar(col, buf)
	case mstype.NVarChar, mstype.NChar:
		_ = readCollation(r)
		r.uint16() // max length, ignoring
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeNChar(buf)
	default:
		badStreamPanicf("Invalid variant typeid")
	}
	panic("shoulnd't get here")
}

// partially length prefixed stream
// http://msdn.microsoft.com/en-us/library/dd340469.aspx
func readPLPType(ti *typeInfo, r *tdsBuffer) interface{} {
	size := r.uint64()
	var buf *bytes.Buffer
	switch size {
	case _PLP_NULL:
		// null
		return nil
	case _UNKNOWN_PLP_LEN:
		// size unknown
		buf = bytes.NewBuffer(make([]byte, 0, 1000))
	default:
		buf = bytes.NewBuffer(make([]byte, 0, size))
	}
	for true {
		chunksize := r.uint32()
		if chunksize == 0 {
			break
		}
		if _, err := io.CopyN(buf, r, int64(chunksize)); err != nil {
			badStreamPanicf("Reading PLP type failed: %s", err.Error())
		}
	}
	switch ti.TypeID {
	case mstype.Xml:
		return decodeXml(*ti, buf.Bytes())
	case mstype.BigVarChar, mstype.BigChar, mstype.Text:
		return decodeChar(ti.Collation, buf.Bytes())
	case mstype.BigVarBin, mstype.BigBinary, mstype.Image:
		return buf.Bytes()
	case mstype.NVarChar, mstype.NChar, mstype.NText:
		return decodeNChar(buf.Bytes())
	case mstype.Udt:
		return decodeUdt(*ti, buf.Bytes())
	}
	panic("shoulnd't get here")
}

func writePLPType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if err = binary.Write(w, binary.LittleEndian, uint64(_UNKNOWN_PLP_LEN)); err != nil {
		return
	}
	for {
		chunksize := uint32(len(buf))
		if chunksize == 0 {
			err = binary.Write(w, binary.LittleEndian, uint32(_PLP_TERMINATOR))
			return
		}
		if err = binary.Write(w, binary.LittleEndian, chunksize); err != nil {
			return
		}
		if _, err = w.Write(buf[:chunksize]); err != nil {
			return
		}
		buf = buf[chunksize:]
	}
}

func readVarLen(ti *typeInfo, r *tdsBuffer) {
	switch ti.TypeID {
	case mstype.DateN:
		ti.Size = 3
		ti.Reader = readByteLenType
		ti.Buffer = make([]byte, ti.Size)
	case mstype.TimeN, mstype.DateTime2N, mstype.DateTimeOffsetN:
		ti.Scale = r.byte()
		switch ti.Scale {
		case 0, 1, 2:
			ti.Size = 3
		case 3, 4:
			ti.Size = 4
		case 5, 6, 7:
			ti.Size = 5
		default:
			badStreamPanicf("Invalid scale for TIME/DATETIME2/DATETIMEOFFSET type")
		}
		switch ti.TypeID {
		case mstype.DateTime2N:
			ti.Size += 3
		case mstype.DateTimeOffsetN:
			ti.Size += 5
		}
		ti.Reader = readByteLenType
		ti.Buffer = make([]byte, ti.Size)
	case mstype.Guid, mstype.IntN, mstype.Decimal, mstype.Numeric,
		mstype.BitN, mstype.DecimalN, mstype.NumericN, mstype.FltN,
		mstype.MoneyN, mstype.DateTimeN, mstype.Char,
		mstype.VarChar, mstype.Binary, mstype.VarBinary:
		// byle len types
		ti.Size = int(r.byte())
		ti.Buffer = make([]byte, ti.Size)
		switch ti.TypeID {
		case mstype.Decimal, mstype.Numeric, mstype.DecimalN, mstype.NumericN:
			ti.Prec = r.byte()
			ti.Scale = r.byte()
		}
		ti.Reader = readByteLenType
	case mstype.Xml:
		ti.XmlInfo.SchemaPresent = r.byte()
		if ti.XmlInfo.SchemaPresent != 0 {
			// dbname
			ti.XmlInfo.DBName = r.BVarChar()
			// owning schema
			ti.XmlInfo.OwningSchema = r.BVarChar()
			// xml schema collection
			ti.XmlInfo.XmlSchemaCollection = r.UsVarChar()
		}
		ti.Reader = readPLPType
	case mstype.Udt:
		ti.Size = int(r.uint16())
		ti.UdtInfo.DBName = r.BVarChar()
		ti.UdtInfo.SchemaName = r.BVarChar()
		ti.UdtInfo.TypeName = r.BVarChar()
		ti.UdtInfo.AssemblyQualifiedName = r.UsVarChar()

		ti.Buffer = make([]byte, ti.Size)
		ti.Reader = readPLPType
	case mstype.BigVarBin, mstype.BigVarChar, mstype.BigBinary, mstype.BigChar,
		mstype.NVarChar, mstype.NChar:
		// short len types
		ti.Size = int(r.uint16())
		switch ti.TypeID {
		case mstype.BigVarChar, mstype.BigChar, mstype.NVarChar, mstype.NChar:
			ti.Collation = readCollation(r)
		}
		if ti.Size == 0xffff {
			ti.Reader = readPLPType
		} else {
			ti.Buffer = make([]byte, ti.Size)
			ti.Reader = readShortLenType
		}
	case mstype.Text, mstype.Image, mstype.NText, mstype.Variant:
		// LONGLEN_TYPE
		ti.Size = int(r.int32())
		switch ti.TypeID {
		case mstype.Text, mstype.NText:
			ti.Collation = readCollation(r)
			// ignore tablenames
			numparts := int(r.byte())
			for i := 0; i < numparts; i++ {
				r.UsVarChar()
			}
			ti.Reader = readLongLenType
		case mstype.Image:
			// ignore tablenames
			numparts := int(r.byte())
			for i := 0; i < numparts; i++ {
				r.UsVarChar()
			}
			ti.Reader = readLongLenType
		case mstype.Variant:
			ti.Reader = readVariantType
		}
	default:
		badStreamPanicf("Invalid type %d", ti.TypeID)
	}
	return
}

func decodeMoney(buf []byte) []byte {
	money := int64(uint64(buf[4]) |
		uint64(buf[5])<<8 |
		uint64(buf[6])<<16 |
		uint64(buf[7])<<24 |
		uint64(buf[0])<<32 |
		uint64(buf[1])<<40 |
		uint64(buf[2])<<48 |
		uint64(buf[3])<<56)
	return scaleBytes(strconv.FormatInt(money, 10), 4)
}

func decodeMoney4(buf []byte) []byte {
	money := int32(binary.LittleEndian.Uint32(buf[0:4]))
	return scaleBytes(strconv.FormatInt(int64(money), 10), 4)
}

func decodeGuid(buf []byte) []byte {
	res := make([]byte, 16)
	copy(res, buf)
	return res
}

func decodeDecimal(prec uint8, scale uint8, buf []byte) []byte {
	var sign uint8
	sign = buf[0]
	dec := Decimal{
		positive: sign != 0,
		prec:     prec,
		scale:    scale,
	}
	buf = buf[1:]
	l := len(buf) / 4
	for i := 0; i < l; i++ {
		dec.integer[i] = binary.LittleEndian.Uint32(buf[0:4])
		buf = buf[4:]
	}
	return dec.Bytes()
}

// http://msdn.microsoft.com/en-us/library/ee780895.aspx
func decodeDateInt(buf []byte) (days int) {
	days = int(buf[0]) + int(buf[1])*256 + int(buf[2])*256*256
	return
}

func decodeDate(buf []byte) time.Time {
	return time.Date(1, 1, 1+decodeDateInt(buf), 0, 0, 0, 0, time.UTC)
}

func encodeDate(val time.Time) (buf []byte) {
	days, _, _ := dateTime2(val)
	buf = make([]byte, 3)
	buf[0] = byte(days)
	buf[1] = byte(days >> 8)
	buf[2] = byte(days >> 16)
	return
}

func decodeTimeInt(scale uint8, buf []byte) (sec int, ns int) {
	var acc uint64 = 0
	for i := len(buf) - 1; i >= 0; i-- {
		acc <<= 8
		acc |= uint64(buf[i])
	}
	for i := 0; i < 7-int(scale); i++ {
		acc *= 10
	}
	nsbig := acc * 100
	sec = int(nsbig / 1000000000)
	ns = int(nsbig % 1000000000)
	return
}

// calculate size of time field in bytes
func calcTimeSize(scale int) int {
	if scale <= 2 {
		return 3
	} else if scale <= 4 {
		return 4
	} else {
		return 5
	}
}

// writes time value into a field buffer
// buffer should be at least calcTimeSize long
func encodeTimeInt(seconds, ns, scale int, buf []byte) {
	ns_total := int64(seconds) * 1000 * 1000 * 1000 + int64(ns)
	t := ns_total / int64(math.Pow10(int(scale)*-1) * 1e9)
	buf[0] = byte(t)
	buf[1] = byte(t >> 8)
	buf[2] = byte(t >> 16)
	buf[3] = byte(t >> 24)
	buf[4] = byte(t >> 32)
}

func decodeTime(scale uint8, buf []byte) time.Time {
	sec, ns := decodeTimeInt(scale, buf)
	return time.Date(1, 1, 1, 0, 0, sec, ns, time.UTC)
}

func encodeTime(hour, minute, second, ns, scale int) (buf []byte) {
	seconds := hour * 3600 + minute * 60 + second
	buf = make([]byte, calcTimeSize(scale))
	encodeTimeInt(seconds, ns, scale, buf)
	return
}

func decodeDateTime2(scale uint8, buf []byte) time.Time {
	timesize := len(buf) - 3
	sec, ns := decodeTimeInt(scale, buf[:timesize])
	days := decodeDateInt(buf[timesize:])
	return time.Date(1, 1, 1+days, 0, 0, sec, ns, time.UTC)
}

func encodeDateTime2(val time.Time, scale int) (buf []byte) {
	days, seconds, ns := dateTime2(val)
	timesize := calcTimeSize(scale)
	buf = make([]byte, 3 + timesize)
	encodeTimeInt(seconds, ns, scale, buf)
	buf[timesize] = byte(days)
	buf[timesize + 1] = byte(days >> 8)
	buf[timesize + 2] = byte(days >> 16)
	return
}

func decodeDateTimeOffset(scale uint8, buf []byte) time.Time {
	timesize := len(buf) - 3 - 2
	sec, ns := decodeTimeInt(scale, buf[:timesize])
	buf = buf[timesize:]
	days := decodeDateInt(buf[:3])
	buf = buf[3:]
	offset := int(int16(binary.LittleEndian.Uint16(buf))) // in mins
	return time.Date(1, 1, 1+days, 0, 0, sec+offset*60, ns,
		time.FixedZone("", offset*60))
}

func encodeDateTimeOffset(val time.Time, scale int) (buf []byte) {
	timesize := calcTimeSize(scale)
	buf = make([]byte, timesize + 2 + 3)
	days, seconds, ns := dateTime2(val.In(time.UTC))
	encodeTimeInt(seconds, ns, scale, buf)
	buf[timesize] = byte(days)
	buf[timesize + 1] = byte(days >> 8)
	buf[timesize + 2] = byte(days >> 16)
	_, offset := val.Zone()
	offset /= 60
	buf[timesize + 3] = byte(offset)
	buf[timesize + 4] = byte(offset >> 8)
	return
}

// returns days since Jan 1st 0001 in Gregorian calendar
func gregorianDays(year, yearday int) int {
	year0 := year - 1
	return year0*365 + year0/4 - year0/100 + year0/400 + yearday - 1
}

func dateTime2(t time.Time) (days int, seconds int, ns int) {
	// days since Jan 1 1 (in same TZ as t)
	days = gregorianDays(t.Year(), t.YearDay())
	seconds = t.Second() + t.Minute() * 60 + t.Hour() * 60 * 60
	ns = t.Nanosecond()
	if days < 0 {
		days = 0
		seconds = 0
		ns = 0
	}
	max := gregorianDays(9999, 365)
	if days > max {
		days = max
		seconds = 59 + 59*60 + 23*60*60
		ns = 999999900
	}
	return
}

func decodeChar(col cp.Collation, buf []byte) string {
	return cp.CharsetToUTF8(col, buf)
}

func decodeUcs2(buf []byte) string {
	res, err := ucs22str(buf)
	if err != nil {
		badStreamPanicf("Invalid UCS2 encoding: %s", err.Error())
	}
	return res
}

func decodeNChar(buf []byte) string {
	return decodeUcs2(buf)
}

func decodeXml(ti typeInfo, buf []byte) string {
	return decodeUcs2(buf)
}

func decodeUdt(ti typeInfo, buf []byte) []byte {
	return buf
}

// makes go/sql type instance as described below
// It should return
// the value type that can be used to scan types into. For example, the database
// column type "bigint" this should return "reflect.TypeOf(int64(0))".
func makeGoLangScanType(ti typeInfo) reflect.Type {
	switch ti.TypeID {
	case mstype.Int1:
		return reflect.TypeOf(int64(0))
	case mstype.Int2:
		return reflect.TypeOf(int64(0))
	case mstype.Int4:
		return reflect.TypeOf(int64(0))
	case mstype.Int8:
		return reflect.TypeOf(int64(0))
	case mstype.Flt4:
		return reflect.TypeOf(float64(0))
	case mstype.IntN:
		switch ti.Size {
		case 1:
			return reflect.TypeOf(int64(0))
		case 2:
			return reflect.TypeOf(int64(0))
		case 4:
			return reflect.TypeOf(int64(0))
		case 8:
			return reflect.TypeOf(int64(0))
		default:
			panic("invalid size of INTNTYPE")
		}
	case mstype.Flt8:
		return reflect.TypeOf(float64(0))
	case mstype.FltN:
		switch ti.Size {
		case 4:
			return reflect.TypeOf(float64(0))
		case 8:
			return reflect.TypeOf(float64(0))
		default:
			panic("invalid size of FLNNTYPE")
		}
	case mstype.BigVarBin:
		return reflect.TypeOf([]byte{})
	case mstype.VarChar:
		return reflect.TypeOf("")
	case mstype.NVarChar:
		return reflect.TypeOf("")
	case mstype.Bit, mstype.BitN:
		return reflect.TypeOf(true)
	case mstype.DecimalN, mstype.NumericN:
		return reflect.TypeOf([]byte{})
	case mstype.Money, mstype.Money4, mstype.MoneyN:
		switch ti.Size {
		case 4:
			return reflect.TypeOf([]byte{})
		case 8:
			return reflect.TypeOf([]byte{})
		default:
			panic("invalid size of MONEYN")
		}
	case mstype.DateTim4:
		return reflect.TypeOf(time.Time{})
	case mstype.DateTime:
		return reflect.TypeOf(time.Time{})
	case mstype.DateTimeN:
		switch ti.Size {
		case 4:
			return reflect.TypeOf(time.Time{})
		case 8:
			return reflect.TypeOf(time.Time{})
		default:
			panic("invalid size of DATETIMEN")
		}
	case mstype.DateTime2N:
		return reflect.TypeOf(time.Time{})
	case mstype.DateN:
		return reflect.TypeOf(time.Time{})
	case mstype.TimeN:
		return reflect.TypeOf(time.Time{})
	case mstype.DateTimeOffsetN:
		return reflect.TypeOf(time.Time{})
	case mstype.BigVarChar:
		return reflect.TypeOf("")
	case mstype.BigChar:
		return reflect.TypeOf("")
	case mstype.NChar:
		return reflect.TypeOf("")
	case mstype.Guid:
		return reflect.TypeOf([]byte{})
	case mstype.Xml:
		return reflect.TypeOf("")
	case mstype.Text:
		return reflect.TypeOf("")
	case mstype.NText:
		return reflect.TypeOf("")
	case mstype.Image:
		return reflect.TypeOf([]byte{})
	case mstype.BigBinary:
		return reflect.TypeOf([]byte{})
	case mstype.Variant:
		return reflect.TypeOf(nil)
	default:
		panic(fmt.Sprintf("not implemented makeGoLangScanType for type %d", ti.TypeID))
	}
}

func makeDecl(ti typeInfo) string {
	switch ti.TypeID {
	case mstype.Null:
		// maybe we should use something else here
		// this is tested in TestNull
		return "nvarchar(1)"
	case mstype.Int1:
		return "tinyint"
	case mstype.Int2:
		return "smallint"
	case mstype.Int4:
		return "int"
	case mstype.Int8:
		return "bigint"
	case mstype.Flt4:
		return "real"
	case mstype.IntN:
		switch ti.Size {
		case 1:
			return "tinyint"
		case 2:
			return "smallint"
		case 4:
			return "int"
		case 8:
			return "bigint"
		default:
			panic("invalid size of INTNTYPE")
		}
	case mstype.Flt8:
		return "float"
	case mstype.FltN:
		switch ti.Size {
		case 4:
			return "real"
		case 8:
			return "float"
		default:
			panic("invalid size of FLNNTYPE")
		}
	case mstype.Decimal, mstype.DecimalN:
		return fmt.Sprintf("decimal(%d, %d)", ti.Prec, ti.Scale)
	case mstype.Numeric, mstype.NumericN:
		return fmt.Sprintf("numeric(%d, %d)", ti.Prec, ti.Scale)
	case mstype.Money4:
		return "smallmoney"
	case mstype.Money:
		return "money"
	case mstype.MoneyN:
		switch ti.Size {
		case 4:
			return "smallmoney"
		case 8:
			return "money"
		default:
			panic("invalid size of MONEYNTYPE")
		}
	case mstype.BigVarBin:
		if ti.Size > 8000 || ti.Size == 0 {
			return "varbinary(max)"
		} else {
			return fmt.Sprintf("varbinary(%d)", ti.Size)
		}
	case mstype.NChar:
		return fmt.Sprintf("nchar(%d)", ti.Size/2)
	case mstype.BigChar, mstype.Char:
		return fmt.Sprintf("char(%d)", ti.Size)
	case mstype.BigVarChar, mstype.VarChar:
		if ti.Size > 4000 || ti.Size == 0 {
			return fmt.Sprintf("varchar(max)")
		} else {
			return fmt.Sprintf("varchar(%d)", ti.Size)
		}
	case mstype.NVarChar:
		if ti.Size > 8000 || ti.Size == 0 {
			return "nvarchar(max)"
		} else {
			return fmt.Sprintf("nvarchar(%d)", ti.Size/2)
		}
	case mstype.Bit, mstype.BitN:
		return "bit"
	case mstype.DateN:
		return "date"
	case mstype.DateTim4:
		return "smalldatetime"
	case mstype.DateTime:
		return "datetime"
	case mstype.DateTimeN:
		switch ti.Size {
		case 4:
			return "smalldatetime"
		case 8:
			return "datetime"
		default:
			panic("invalid size of DATETIMNTYPE")
		}
	case mstype.TimeN:
		return "time"
	case mstype.DateTime2N:
		return fmt.Sprintf("datetime2(%d)", ti.Scale)
	case mstype.DateTimeOffsetN:
		return fmt.Sprintf("datetimeoffset(%d)", ti.Scale)
	case mstype.Text:
		return "text"
	case mstype.NText:
		return "ntext"
	case mstype.Udt:
		return ti.UdtInfo.TypeName
	case mstype.Guid:
		return "uniqueidentifier"
	default:
		panic(fmt.Sprintf("not implemented makeDecl for type %#x", ti.TypeID))
	}
}

// makes go/sql type name as described below
// RowsColumnTypeDatabaseTypeName may be implemented by Rows. It should return the
// database system type name without the length. Type names should be uppercase.
// Examples of returned types: "VARCHAR", "NVARCHAR", "VARCHAR2", "CHAR", "TEXT",
// "DECIMAL", "SMALLINT", "INT", "BIGINT", "BOOL", "[]BIGINT", "JSONB", "XML",
// "TIMESTAMP".
func makeGoLangTypeName(ti typeInfo) string {
	switch ti.TypeID {
	case mstype.Int1:
		return "TINYINT"
	case mstype.Int2:
		return "SMALLINT"
	case mstype.Int4:
		return "INT"
	case mstype.Int8:
		return "BIGINT"
	case mstype.Flt4:
		return "REAL"
	case mstype.IntN:
		switch ti.Size {
		case 1:
			return "TINYINT"
		case 2:
			return "SMALLINT"
		case 4:
			return "INT"
		case 8:
			return "BIGINT"
		default:
			panic("invalid size of INTNTYPE")
		}
	case mstype.Flt8:
		return "FLOAT"
	case mstype.FltN:
		switch ti.Size {
		case 4:
			return "REAL"
		case 8:
			return "FLOAT"
		default:
			panic("invalid size of FLNNTYPE")
		}
	case mstype.BigVarBin:
		return "VARBINARY"
	case mstype.VarChar:
		return "VARCHAR"
	case mstype.NVarChar:
		return "NVARCHAR"
	case mstype.Bit, mstype.BitN:
		return "BIT"
	case mstype.DecimalN, mstype.NumericN:
		return "DECIMAL"
	case mstype.Money, mstype.Money4, mstype.MoneyN:
		switch ti.Size {
		case 4:
			return "SMALLMONEY"
		case 8:
			return "MONEY"
		default:
			panic("invalid size of MONEYN")
		}
	case mstype.DateTim4:
		return "SMALLDATETIME"
	case mstype.DateTime:
		return "DATETIME"
	case mstype.DateTimeN:
		switch ti.Size {
		case 4:
			return "SMALLDATETIME"
		case 8:
			return "DATETIME"
		default:
			panic("invalid size of DATETIMEN")
		}
	case mstype.DateTime2N:
		return "DATETIME2"
	case mstype.DateN:
		return "DATE"
	case mstype.TimeN:
		return "TIME"
	case mstype.DateTimeOffsetN:
		return "DATETIMEOFFSET"
	case mstype.BigVarChar:
		return "VARCHAR"
	case mstype.BigChar:
		return "CHAR"
	case mstype.NChar:
		return "NCHAR"
	case mstype.Guid:
		return "UNIQUEIDENTIFIER"
	case mstype.Xml:
		return "XML"
	case mstype.Text:
		return "TEXT"
	case mstype.NText:
		return "NTEXT"
	case mstype.Image:
		return "IMAGE"
	case mstype.Variant:
		return "SQL_VARIANT"
	case mstype.BigBinary:
		return "BINARY"
	default:
		panic(fmt.Sprintf("not implemented makeGoLangTypeName for type %d", ti.TypeID))
	}
}

// makes go/sql type length as described below
// It should return the length
// of the column type if the column is a variable length type. If the column is
// not a variable length type ok should return false.
// If length is not limited other than system limits, it should return math.MaxInt64.
// The following are examples of returned values for various types:
//   TEXT          (math.MaxInt64, true)
//   varchar(10)   (10, true)
//   nvarchar(10)  (10, true)
//   decimal       (0, false)
//   int           (0, false)
//   bytea(30)     (30, true)
func makeGoLangTypeLength(ti typeInfo) (int64, bool) {
	switch ti.TypeID {
	case mstype.Int1:
		return 0, false
	case mstype.Int2:
		return 0, false
	case mstype.Int4:
		return 0, false
	case mstype.Int8:
		return 0, false
	case mstype.Flt4:
		return 0, false
	case mstype.IntN:
		switch ti.Size {
		case 1:
			return 0, false
		case 2:
			return 0, false
		case 4:
			return 0, false
		case 8:
			return 0, false
		default:
			panic("invalid size of INTNTYPE")
		}
	case mstype.Flt8:
		return 0, false
	case mstype.FltN:
		switch ti.Size {
		case 4:
			return 0, false
		case 8:
			return 0, false
		default:
			panic("invalid size of FLNNTYPE")
		}
	case mstype.Bit, mstype.BitN:
		return 0, false
	case mstype.DecimalN, mstype.NumericN:
		return 0, false
	case mstype.Money, mstype.Money4, mstype.MoneyN:
		switch ti.Size {
		case 4:
			return 0, false
		case 8:
			return 0, false
		default:
			panic("invalid size of MONEYN")
		}
	case mstype.DateTim4, mstype.DateTime:
		return 0, false
	case mstype.DateTimeN:
		switch ti.Size {
		case 4:
			return 0, false
		case 8:
			return 0, false
		default:
			panic("invalid size of DATETIMEN")
		}
	case mstype.DateTime2N:
		return 0, false
	case mstype.DateN:
		return 0, false
	case mstype.TimeN:
		return 0, false
	case mstype.DateTimeOffsetN:
		return 0, false
	case mstype.BigVarBin:
		if ti.Size == 0xffff {
			return 2147483645, true
		} else {
			return int64(ti.Size), true
		}
	case mstype.VarChar:
		return int64(ti.Size), true
	case mstype.BigVarChar:
		if ti.Size == 0xffff {
			return 2147483645, true
		} else {
			return int64(ti.Size), true
		}
	case mstype.BigChar:
		return int64(ti.Size), true
	case mstype.NVarChar:
		if ti.Size == 0xffff {
			return 2147483645 / 2, true
		} else {
			return int64(ti.Size) / 2, true
		}
	case mstype.NChar:
		return int64(ti.Size) / 2, true
	case mstype.Guid:
		return 0, false
	case mstype.Xml:
		return 1073741822, true
	case mstype.Text:
		return 2147483647, true
	case mstype.NText:
		return 1073741823, true
	case mstype.Image:
		return 2147483647, true
	case mstype.Variant:
		return 0, false
	case mstype.BigBinary:
		return 0, false
	default:
		panic(fmt.Sprintf("not implemented makeGoLangTypeLength for type %d", ti.TypeID))
	}
}

// makes go/sql type precision and scale as described below
// It should return the length
// of the column type if the column is a variable length type. If the column is
// not a variable length type ok should return false.
// If length is not limited other than system limits, it should return math.MaxInt64.
// The following are examples of returned values for various types:
//   TEXT          (math.MaxInt64, true)
//   varchar(10)   (10, true)
//   nvarchar(10)  (10, true)
//   decimal       (0, false)
//   int           (0, false)
//   bytea(30)     (30, true)
func makeGoLangTypePrecisionScale(ti typeInfo) (int64, int64, bool) {
	switch ti.TypeID {
	case mstype.Int1:
		return 0, 0, false
	case mstype.Int2:
		return 0, 0, false
	case mstype.Int4:
		return 0, 0, false
	case mstype.Int8:
		return 0, 0, false
	case mstype.Flt4:
		return 0, 0, false
	case mstype.IntN:
		switch ti.Size {
		case 1:
			return 0, 0, false
		case 2:
			return 0, 0, false
		case 4:
			return 0, 0, false
		case 8:
			return 0, 0, false
		default:
			panic("invalid size of INTNTYPE")
		}
	case mstype.Flt8:
		return 0, 0, false
	case mstype.FltN:
		switch ti.Size {
		case 4:
			return 0, 0, false
		case 8:
			return 0, 0, false
		default:
			panic("invalid size of FLNNTYPE")
		}
	case mstype.Bit, mstype.BitN:
		return 0, 0, false
	case mstype.DecimalN, mstype.NumericN:
		return int64(ti.Prec), int64(ti.Scale), true
	case mstype.Money, mstype.Money4, mstype.MoneyN:
		switch ti.Size {
		case 4:
			return 0, 0, false
		case 8:
			return 0, 0, false
		default:
			panic("invalid size of MONEYN")
		}
	case mstype.DateTim4, mstype.DateTime:
		return 0, 0, false
	case mstype.DateTimeN:
		switch ti.Size {
		case 4:
			return 0, 0, false
		case 8:
			return 0, 0, false
		default:
			panic("invalid size of DATETIMEN")
		}
	case mstype.DateTime2N:
		return 0, 0, false
	case mstype.DateN:
		return 0, 0, false
	case mstype.TimeN:
		return 0, 0, false
	case mstype.DateTimeOffsetN:
		return 0, 0, false
	case mstype.BigVarBin:
		return 0, 0, false
	case mstype.VarChar:
		return 0, 0, false
	case mstype.BigVarChar:
		return 0, 0, false
	case mstype.BigChar:
		return 0, 0, false
	case mstype.NVarChar:
		return 0, 0, false
	case mstype.NChar:
		return 0, 0, false
	case mstype.Guid:
		return 0, 0, false
	case mstype.Xml:
		return 0, 0, false
	case mstype.Text:
		return 0, 0, false
	case mstype.NText:
		return 0, 0, false
	case mstype.Image:
		return 0, 0, false
	case mstype.Variant:
		return 0, 0, false
	case mstype.BigBinary:
		return 0, 0, false
	default:
		panic(fmt.Sprintf("not implemented makeGoLangTypePrecisionScale for type %d", ti.TypeID))
	}
}
