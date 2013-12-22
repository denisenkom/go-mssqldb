package mssql

import (
    "io"
    "encoding/binary"
)


// fixed-length data types
// http://msdn.microsoft.com/en-us/library/dd341171.aspx
const (
    typeNull = 0x1f
    typeInt1 = 0x30
    typeBit = 0x32
    typeInt2 = 0x34
    typeInt4 = 0x38
    typeDateTim4 = 0x3a
    typeFlt4 = 0x3b
    typeMoney = 0x3c
    typeDateTime = 0x3d
    typeFlt8 = 0x3e
    typeMoney4 = 0x7a
    typeInt8 = 0x7f
)

// variable-length data types
// http://msdn.microsoft.com/en-us/library/dd358341.aspx
const (
    typeGuid = 0x24
    typeIntN = 0x26
    typeDecimal = 0x37  // legacy
    typeNumeric = 0x3f  // legacy
    typeBitN = 0x68
    typeDecimalN = 0x6a
    typeNumericN = 0x6c
    typeFltN = 0x6d
    typeMoneyN = 0x6e
    typeDateTimeN = 0x6f
    typeDateN = 0x28
    typeTimeN = 0x29
    typeDateTime2N = 0x2a
    typeDateTimeOffsetN = 0x2b
    typeChar = 0x2f // legacy
    typeVarChar = 0x27 // legacy
    typeBinary = 0x2d // legacy
    typeVarBinary = 0x25 // legacy

    typeBigVarBin = 0xa5
    typeBigVarChar = 0xa7
    typeBigBinary = 0xad
    typeBigChar = 0xaf
    typeNVarChar = 0xe7
    typeNCharType = 0xef
    typeXml = 0xf1
    typeUdt = 0xf0

    typeText = 0x23
    typeImage = 0x22
    typeNText = 0x63
    typeVariant = 0x62
)


type typeInfoIface interface {
    readData(r io.Reader) (value interface{}, err error)
}

type typeParser func(typeid uint8, r io.Reader) (typeInfoIface, error)


type typeInfoBitN struct {}

func (t typeInfoBitN)readData(r io.Reader) (value interface{}, err error) {
    var size uint8
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    switch size {
    case 0:
        return nil, nil
    case 1:
        var ivalue uint8
        err = binary.Read(r, binary.LittleEndian, &ivalue)
        if err != nil {
            return nil, err
        }
        return ivalue != 0, nil
    default:
        return nil, streamErrorf("Invalid BITNTYPE size: %d", size)
    }
}

func typeBitNParser(typeid uint8, r io.Reader) (res typeInfoIface, err error) {
    var size uint8
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    if size != 1 {
        return nil, streamErrorf("Invalid BITNTYPE size: %d", size)
    }
    return typeInfoBitN{}, nil
}


type typeInfoInt4 struct {}

var typeInfoInt4Instance = typeInfoInt4{}

func (t typeInfoInt4)readData(r io.Reader) (value interface{}, err error) {
    var ivalue int32
    err = binary.Read(r, binary.LittleEndian, &ivalue)
    return ivalue, err
}

func typeInt4Parser(typeid uint8, r io.Reader) (res typeInfoIface, err error) {
    return typeInfoInt4Instance, nil
}


type typeInfoIntN struct {
    size uint8
}

func (t typeInfoIntN)readData(r io.Reader) (value interface{}, err error) {
    var size uint8
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    var int1 int8
    var int2 int16
    var int4 int32
    var int8 int64
    switch size {
    case 0:
        return nil, nil
    case 1:
        err = binary.Read(r, binary.LittleEndian, &int1); if err != nil {
            return nil, err
        }
        return int1, nil
    case 2:
        err = binary.Read(r, binary.LittleEndian, &int2); if err != nil {
            return nil, err
        }
        return int2, nil
    case 4:
        err = binary.Read(r, binary.LittleEndian, &int4); if err != nil {
            return nil, err
        }
        return int4, nil
    case 8:
        err = binary.Read(r, binary.LittleEndian, &int8); if err != nil {
            return nil, err
        }
        return int8, nil
    default:
        return nil, streamErrorf("Invalid INTNTYPE size: %d", size)
    }
}

func typeIntNParser(typeid uint8, r io.Reader) (typeInfoIface, error) {
    res := typeInfoIntN{}
    err := binary.Read(r, binary.LittleEndian, &res.size)
    if err != nil {
        return nil, err
    }
    return res, nil
}


type typeInfoBigVarChar struct {
    size uint16
    collation collation
}

func (t typeInfoBigVarChar)readData(r io.Reader) (value interface{}, err error) {
    var size uint16
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    if size == 0xffff {
        return nil, nil
    }
    buf := make([]byte, size)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return nil, err
    }
    return string(buf), nil
}

func typeBigVarCharParser(typeid uint8, r io.Reader) (typeInfoIface, error) {
    res := typeInfoBigVarChar{}
    err := binary.Read(r, binary.LittleEndian, &res.size)
    if err != nil {
        return nil, err
    }
    res.collation, err = readCollation(r)
    if err != nil {
        return nil, err
    }
    return &res, nil
}


type typeInfoNVarChar struct {
    size uint16
    collation collation
}

func (t typeInfoNVarChar)readData(r io.Reader) (value interface{}, err error) {
    var size uint16
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    if size == 0xffff {
        return nil, nil
    }
    buf := make([]byte, size)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return nil, err
    }
    return ucs22utf8.ConvertString(string(buf))
}

func typeNVarCharParser(typeid uint8, r io.Reader) (typeInfoIface, error) {
    res := typeInfoNVarChar{}
    err := binary.Read(r, binary.LittleEndian, &res.size)
    if err != nil {
        return nil, err
    }
    res.collation, err = readCollation(r)
    if err != nil {
        return nil, err
    }
    return &res, nil
}


type typeInfoFltN struct {
    size uint8
}

func (t typeInfoFltN)readData(r io.Reader) (value interface{}, err error) {
    var size uint8
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    var flt4 float32
    var flt8 float64
    switch size {
    case 0:
        return nil, nil
    case 4:
        err = binary.Read(r, binary.LittleEndian, &flt4)
        if err != nil {
            return nil, err
        }
        return flt4, nil
    case 8:
        err = binary.Read(r, binary.LittleEndian, &flt8)
        if err != nil {
            return nil, err
        }
        return flt8, nil
    default:
        return nil, streamErrorf("Invalid FLTNTYPE size: %d", size)
    }
}

func typeFltNParser(typeid uint8, r io.Reader) (typeInfoIface, error) {
    res := typeInfoFltN{}
    err := binary.Read(r, binary.LittleEndian, &res.size)
    if err != nil {
        return nil, err
    }
    return res, nil
}


type typeInfoDecimalN struct {
    Size uint8
    Prec uint8
    Scale uint8
}

func (t typeInfoDecimalN)readData(r io.Reader) (value interface{}, err error) {
    var size uint8
    err = binary.Read(r, binary.LittleEndian, &size); if err != nil {
        return
    }
    if size == 0 {
        return nil, nil
    }
    var sign uint8
    err = binary.Read(r, binary.LittleEndian, &sign); if err != nil {
        return
    }
    size--
    dec := Decimal{
        positive: sign != 0,
        prec: t.Prec,
        scale: t.Scale,
    }
    err = binary.Read(r, binary.LittleEndian, dec.integer[:size/4]); if err != nil {
        return
    }
    value = dec
    return
}

func typeDecimalNParser(typeid uint8, r io.Reader) (res typeInfoIface, err error) {
    ti := typeInfoDecimalN{}
    res = &ti
    err = binary.Read(r, binary.LittleEndian, &ti)
    return
}
