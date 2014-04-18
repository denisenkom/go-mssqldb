package mssql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

// fixed-length data types
// http://msdn.microsoft.com/en-us/library/dd341171.aspx
const (
	typeNull     = 0x1f
	typeInt1     = 0x30
	typeBit      = 0x32
	typeInt2     = 0x34
	typeInt4     = 0x38
	typeDateTim4 = 0x3a
	typeFlt4     = 0x3b
	typeMoney    = 0x3c
	typeDateTime = 0x3d
	typeFlt8     = 0x3e
	typeMoney4   = 0x7a
	typeInt8     = 0x7f
)

// variable-length data types
// http://msdn.microsoft.com/en-us/library/dd358341.aspx
const (
	// byte len types
	typeGuid            = 0x24
	typeIntN            = 0x26
	typeDecimal         = 0x37 // legacy
	typeNumeric         = 0x3f // legacy
	typeBitN            = 0x68
	typeDecimalN        = 0x6a
	typeNumericN        = 0x6c
	typeFltN            = 0x6d
	typeMoneyN          = 0x6e
	typeDateTimeN       = 0x6f
	typeDateN           = 0x28
	typeTimeN           = 0x29
	typeDateTime2N      = 0x2a
	typeDateTimeOffsetN = 0x2b
	typeChar            = 0x2f // legacy
	typeVarChar         = 0x27 // legacy
	typeBinary          = 0x2d // legacy
	typeVarBinary       = 0x25 // legacy

	// short length types
	typeBigVarBin  = 0xa5
	typeBigVarChar = 0xa7
	typeBigBinary  = 0xad
	typeBigChar    = 0xaf
	typeNVarChar   = 0xe7
	typeNChar      = 0xef
	typeXml        = 0xf1
	typeUdt        = 0xf0

	// long length types
	typeText    = 0x23
	typeImage   = 0x22
	typeNText   = 0x63
	typeVariant = 0x62
)

// TYPE_INFO rule
// http://msdn.microsoft.com/en-us/library/dd358284.aspx
type typeInfo struct {
	TypeId    uint8
	Size      int
	Scale     uint8
	Prec      uint8
	Buffer    []byte
	Collation collation
	Reader    func(ti *typeInfo, r *tdsBuffer) (res []byte)
	Writer    func(w io.Writer, ti typeInfo, buf []byte) (err error)
}

func readTypeInfo(r *tdsBuffer) (res typeInfo) {
	res.TypeId = r.byte()
	switch res.TypeId {
	case typeNull, typeInt1, typeBit, typeInt2, typeInt4, typeDateTim4,
		typeFlt4, typeMoney, typeDateTime, typeFlt8, typeMoney4, typeInt8:
		// those are fixed length types
		switch res.TypeId {
		case typeNull:
			res.Size = 0
		case typeInt1, typeBit:
			res.Size = 1
		case typeInt2:
			res.Size = 2
		case typeInt4, typeDateTim4, typeFlt4, typeMoney4:
			res.Size = 4
		case typeMoney, typeDateTime, typeFlt8, typeInt8:
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
	err = binary.Write(w, binary.LittleEndian, ti.TypeId)
	if err != nil {
		return
	}
	switch ti.TypeId {
	case typeNull, typeInt1, typeBit, typeInt2, typeInt4, typeDateTim4,
		typeFlt4, typeMoney, typeDateTime, typeFlt8, typeMoney4, typeInt8:
		// those are fixed length types
	default: // all others are VARLENTYPE
		err = writeVarLen(w, ti)
		if err != nil {
			return
		}
	}
	return
}

func writeVarLen(w io.Writer, ti *typeInfo) (err error) {
	switch ti.TypeId {
	case typeDateN:

	case typeTimeN, typeDateTime2N, typeDateTimeOffsetN:
		if err = binary.Write(w, binary.LittleEndian, ti.Scale); err != nil {
			return
		}
	case typeGuid, typeIntN, typeDecimal, typeNumeric,
		typeBitN, typeDecimalN, typeNumericN, typeFltN,
		typeMoneyN, typeDateTimeN, typeChar,
		typeVarChar, typeBinary, typeVarBinary:
		// byle len types
		if ti.Size > 0xff {
			panic("Invalid size for BYLELEN_TYPE")
		}
		if err = binary.Write(w, binary.LittleEndian, uint8(ti.Size)); err != nil {
			return
		}
		switch ti.TypeId {
		case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
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
	case typeBigVarBin, typeBigVarChar, typeBigBinary, typeBigChar,
		typeNVarChar, typeNChar, typeXml, typeUdt:
		// short len types
		if ti.Size > 8000 {
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
		switch ti.TypeId {
		case typeBigVarChar, typeBigChar, typeNVarChar, typeNChar:
			if err = writeCollation(w, ti.Collation); err != nil {
				return
			}
		case typeXml:
			var schemapresent uint8 = 0
			if err = binary.Write(w, binary.LittleEndian, schemapresent); err != nil {
				return
			}
		}
	case typeText, typeImage, typeNText, typeVariant:
		// LONGLEN_TYPE
		panic("LONGLEN_TYPE not implemented")
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

func decodeDateTime(buf []byte) time.Time {
	days := int32(binary.LittleEndian.Uint32(buf))
	tm := binary.LittleEndian.Uint32(buf[4:])
	ns := int(math.Trunc(float64(tm%300*10000000) / 3.0))
	secs := int(tm / 300)
	return time.Date(1900, 1, 1+int(days),
		0, 0, secs, ns, time.UTC)
}

func readFixedType(ti *typeInfo, r *tdsBuffer) (res []byte) {
	r.ReadFull(ti.Buffer)
	return ti.Buffer
}

func writeFixedType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	_, err = w.Write(buf)
	return
}

func readByteLenType(ti *typeInfo, r *tdsBuffer) (res []byte) {
	size := r.byte()
	if size == 0 {
		return nil
	}
	r.ReadFull(ti.Buffer[:size])
	return ti.Buffer[:size]
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

func readShortLenType(ti *typeInfo, r *tdsBuffer) (res []byte) {
	size := r.uint16()
	if size == 0xffff {
		return nil
	}
	r.ReadFull(ti.Buffer[:size])
	return ti.Buffer[:size]
}

func writeShortLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if buf == nil {
		err = binary.Write(w, binary.LittleEndian, uint16(0xffff))
		if err != nil {
			return
		}
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

func readLongLenType(ti *typeInfo, r *tdsBuffer) (res []byte) {
	// information about this format can be found here:
	// http://msdn.microsoft.com/en-us/library/dd304783.aspx
	// and here:
	// http://msdn.microsoft.com/en-us/library/dd357254.aspx
	textptrsize := r.byte()
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
	return buf
}

// partially length prefixed stream
// http://msdn.microsoft.com/en-us/library/dd340469.aspx
func readPLPType(ti *typeInfo, r *tdsBuffer) (res []byte) {
	size := r.uint64()
	var buf *bytes.Buffer
	switch size {
	case 0xffffffffffffffff:
		// null
		return nil
	case 0xfffffffffffffffe:
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
	return buf.Bytes()
}

func writePLPType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if buf == nil {
		if err = binary.Write(w, binary.LittleEndian, uint64(0xffffffffffffffff)); err != nil {
			return
		}
		return
	}
	if err = binary.Write(w, binary.LittleEndian, uint64(len(buf))); err != nil {
		return
	}
	for {
		chunksize := uint32(len(buf))
		if err = binary.Write(w, binary.LittleEndian, chunksize); err != nil {
			return
		}
		if chunksize == 0 {
			return
		}
		if _, err = w.Write(buf[:chunksize]); err != nil {
			return
		}
		buf = buf[chunksize:]
	}
}

func readVarLen(ti *typeInfo, r *tdsBuffer) {
	switch ti.TypeId {
	case typeDateN:
		ti.Size = 3
		ti.Reader = readByteLenType
		ti.Buffer = make([]byte, ti.Size)
	case typeTimeN, typeDateTime2N, typeDateTimeOffsetN:
		ti.Scale = r.byte()
		switch ti.Scale {
		case 1, 2:
			ti.Size = 3
		case 3, 4:
			ti.Size = 4
		case 5, 6, 7:
			ti.Size = 5
		default:
			badStreamPanicf("Invalid scale for TIME/DATETIME2/DATETIMEOFFSET type")
		}
		switch ti.TypeId {
		case typeDateTime2N:
			ti.Size += 3
		case typeDateTimeOffsetN:
			ti.Size += 5
		}
		ti.Reader = readByteLenType
		ti.Buffer = make([]byte, ti.Size)
	case typeGuid, typeIntN, typeDecimal, typeNumeric,
		typeBitN, typeDecimalN, typeNumericN, typeFltN,
		typeMoneyN, typeDateTimeN, typeChar,
		typeVarChar, typeBinary, typeVarBinary:
		// byle len types
		ti.Size = int(r.byte())
		ti.Buffer = make([]byte, ti.Size)
		switch ti.TypeId {
		case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
			ti.Prec = r.byte()
			ti.Scale = r.byte()
		}
		ti.Reader = readByteLenType
	case typeXml:
		schemapresent := r.byte()
		if schemapresent != 0 {
			// just ignore this for now
			// dbname
			r.BVarChar()
			// owning schema
			r.BVarChar()
			// xml schema collection
			r.UsVarChar()
		}
		ti.Reader = readPLPType
	case typeBigVarBin, typeBigVarChar, typeBigBinary, typeBigChar,
		typeNVarChar, typeNChar, typeUdt:
		// short len types
		ti.Size = int(r.uint16())
		switch ti.TypeId {
		case typeBigVarChar, typeBigChar, typeNVarChar, typeNChar:
			ti.Collation = readCollation(r)
		}
		if ti.Size == 0xffff {
			ti.Reader = readPLPType
		} else {
			ti.Buffer = make([]byte, ti.Size)
			ti.Reader = readShortLenType
		}
	case typeText, typeImage, typeNText, typeVariant:
		// LONGLEN_TYPE
		ti.Size = int(r.int32())
		switch ti.TypeId {
		case typeText, typeNText:
			ti.Collation = readCollation(r)
		case typeXml:
			panic("XMLTYPE not implemented")
		}
		// ignore tablenames
		numparts := int(r.byte())
		for i := 0; i < numparts; i++ {
			r.UsVarChar()
		}
		ti.Reader = readLongLenType
	default:
		badStreamPanicf("Invalid type %d", ti.TypeId)
	}
	return
}

func decodeMoney(buf []byte) int {
	panic("Not implemented")
}

func decodeMoney4(buf []byte) int {
	panic("Not implemented")
}

func decodeGuid(buf []byte) (res [16]byte) {
	copy(res[:], buf)
	return
}

func decodeDecimal(ti typeInfo, buf []byte) []byte {
	var sign uint8
	sign = buf[0]
	dec := Decimal{
		positive: sign != 0,
		prec:     ti.Prec,
		scale:    ti.Scale,
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
	return int(buf[0]) + int(buf[1])*256 + int(buf[2])*256*256
}

func decodeDate(buf []byte) time.Time {
	return time.Date(1, 1, 1+decodeDateInt(buf), 0, 0, 0, 0, time.UTC)
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

func decodeTime(ti typeInfo, buf []byte) time.Time {
	sec, ns := decodeTimeInt(ti.Scale, buf)
	return time.Date(1, 1, 1, 0, 0, sec, ns, time.UTC)
}

func decodeDateTime2(scale uint8, buf []byte) time.Time {
	timesize := len(buf) - 3
	sec, ns := decodeTimeInt(scale, buf[:timesize])
	days := decodeDateInt(buf[timesize:])
	return time.Date(1, 1, 1+days, 0, 0, sec, ns, time.UTC)
}

func decodeDateTimeOffset(scale uint8, buf []byte) time.Time {
	timesize := len(buf) - 3 - 2
	sec, ns := decodeTimeInt(scale, buf[:timesize])
	buf = buf[timesize:]
	days := decodeDateInt(buf[:3])
	buf = buf[3:]
	offset := int(int16(binary.LittleEndian.Uint16(buf))) // in mins
	return time.Date(1, 1, 1+days, 0, 0, sec, ns,
		time.FixedZone("", offset*60))
}

func decodeChar(ti typeInfo, buf []byte) string {
	return string(buf)
}

func decodeUcs2(buf []byte) string {
	res, err := ucs22str(buf)
	if err != nil {
		badStreamPanicf("Invalid UCS2 encoding: %s", err.Error())
	}
	return res
}

func decodeNChar(ti typeInfo, buf []byte) string {
	return decodeUcs2(buf)
}

func decodeXml(ti typeInfo, buf []byte) string {
	return decodeUcs2(buf)
}

func decodeUdt(ti typeInfo, buf []byte) int {
	panic("Not implemented")
}

func makeDecl(ti typeInfo) string {
	switch ti.TypeId {
	case typeInt8:
		return "bigint"
	case typeFlt4:
		return "real"
	case typeIntN:
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
	case typeFlt8:
		return "float"
	case typeFltN:
		switch ti.Size {
		case 4:
			return "real"
		case 8:
			return "float"
		default:
			panic("invalid size of FLNNTYPE")
		}
	case typeBigVarBin:
		if ti.Size > 8000 {
			return fmt.Sprintf("varbinary(max)")
		} else {
			return fmt.Sprintf("varbinary(%d)", ti.Size)
		}
	case typeNVarChar:
		if ti.Size > 8000 {
			return fmt.Sprintf("nvarchar(max)")
		} else {
			return fmt.Sprintf("nvarchar(%d)", ti.Size/2)
		}
	case typeBit, typeBitN:
		return "bit"
	default:
		panic(fmt.Sprintf("not implemented makeDecl for type %d", ti.TypeId))
	}
}
