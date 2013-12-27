package mssql

import (
    "io"
    "encoding/binary"
    "strings"
    "strconv"
    "math"
)

// token ids
const (
    tokenReturnStatus = 121  // 0x79
    tokenColMetadata = 129  // 0x81
    tokenError = 170  // 0xAA
    tokenLoginAck = 173  // 0xad
    tokenRow = 209  // 0xd1
    tokenEnvChange = 227  // 0xE3
    tokenDone = 253  // 0xFD
    tokenDoneProc = 254
    tokenDoneInProc = 255
    )

// done flags
const (
    doneFinal = 0
    doneMore = 1
    doneError = 2
    doneInxact = 4
    doneCount = 0x10
    doneAttn = 0x20
    doneSrvError = 0x100
)


// ENVCHANGE types
// http://msdn.microsoft.com/en-us/library/dd303449.aspx
const (
    envTypDatabase = 1
    envTypLanguage = 2
    envTypCharset = 3
    envTypPacketSize = 4
    envTypBeginTran = 8
    envTypCommitTran = 9
    envTypRollbackTran = 10
)


// interface for all tokens
type tokenStruct interface{}


type doneStruct struct {
    Status uint16
    CurCmd uint16
    RowCount uint64
}


type doneInProcStruct doneStruct

var doneFlags2str = map[uint16]string{
    doneFinal: "final",
    doneMore: "more",
    doneError: "error",
    doneInxact: "inxact",
    doneCount: "count",
    doneAttn: "attn",
    doneSrvError: "srverror",
}

func doneFlags2Str(flags uint16) string {
    strs := make([]string, 0, len(doneFlags2str))
    for flag, tag := range doneFlags2str {
        if flags & flag != 0 {
            strs = append(strs, tag)
        }
    }
    return strings.Join(strs, "|")
}


// ENVCHANGE stream
// http://msdn.microsoft.com/en-us/library/dd303449.aspx
func processEnvChg(sess *tdsSession) (err error) {
    r := io.Reader(sess.buf)
    var size uint16
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return err
    }
    r = &io.LimitedReader{r, int64(size)}
    for true {
        var envtype uint8
        err = binary.Read(r, binary.LittleEndian, &envtype)
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }
        switch envtype {
        case envTypDatabase:
            _, err = readBVarChar(r)
            if err != nil {
                return err
            }
            sess.database, err = readBVarChar(r)
            if err != nil {
                return err
            }
        case envTypPacketSize:
            packetsize, err := readBVarChar(r)
            if err != nil {
                return err
            }
            _, err = readBVarChar(r)
            if err != nil {
                return err
            }
            packetsizei, err := strconv.Atoi(packetsize)
            if err != nil {
                return streamErrorf("Invalid Packet size value returned from server (%s): %s", packetsize, err.Error())
            }
            if len(sess.buf.buf) != packetsizei {
                newbuf := make([]byte, packetsizei)
                copy(newbuf, sess.buf.buf)
                sess.buf.buf = newbuf
            }
        case envTypBeginTran:
            tranid, err := readBVarByte(r)
            if len(tranid) != 8 {
                return streamErrorf("invalid size of transaction identifier: %d", len(tranid))
            }
            sess.tranid = binary.LittleEndian.Uint64(tranid)
            if err != nil {
                return err
            }
            _, err = readBVarByte(r)
            if err != nil {
                return err
            }
        case envTypCommitTran, envTypRollbackTran:
            _, err = readBVarByte(r)
            if err != nil {
                return err
            }
            _, err = readBVarByte(r)
            if err != nil {
                return err
            }
            sess.tranid = 0
        default:
            return streamErrorf("unknown env type: %d", envtype)
        }

    }
    return nil
}


type returnStatus int32


// http://msdn.microsoft.com/en-us/library/dd358180.aspx
func parseReturnStatus(r *tdsBuffer) returnStatus {
    return returnStatus(r.int32())
}


func parseDone(r *tdsBuffer) (res doneStruct) {
    res.Status = r.uint16()
    res.CurCmd = r.uint16()
    res.RowCount = r.uint64()
    return res
}


func parseDoneInProc(r *tdsBuffer) (res doneInProcStruct) {
    res.Status = r.uint16()
    res.CurCmd = r.uint16()
    res.RowCount = r.uint64()
    return res
}


type loginAckStruct struct {
    Interface uint8
    TDSVersion uint32
    ProgName string
    ProgVer uint32
}

func parseLoginAck(r *tdsBuffer) loginAckStruct {
    size := r.uint16()
    buf := make([]byte, size)
    r.ReadFull(buf)
    var res loginAckStruct
    res.Interface = buf[0]
    res.TDSVersion = binary.BigEndian.Uint32(buf[1:])
    prognamelen := buf[1+4]
    var err error
    if res.ProgName, err = ucs22str(buf[1+4+1:1+4+1 + prognamelen * 2]); err != nil {
        badStreamPanic(err)
    }
    res.ProgVer = binary.BigEndian.Uint32(buf[size - 4:])
    return res
}


// http://msdn.microsoft.com/en-us/library/dd357363.aspx
func parseColMetadata72(r *tdsBuffer) (columns []columnStruct) {
    count := r.uint16()
    if count == 0xffff {
        // no metadata is sent
        return nil
    }
    columns = make([]columnStruct, count)
    for i := range columns {
        column := &columns[i]
        column.UserType = r.uint32()
        column.Flags = r.uint16()

        // parsing TYPE_INFO structure
        column.ti = readTypeInfo(r)
        column.ColName = r.BVarChar()
    }
    return columns
}


// http://msdn.microsoft.com/en-us/library/dd357254.aspx
func parseRow(r io.Reader, columns []columnStruct) (row []interface{}, err error) {
    row = make([]interface{}, len(columns))
    for i, column := range columns {
        var buf []byte
        buf, err = column.ti.Reader(&column.ti, r); if err != nil {
            return
        }
        if buf == nil {
            row[i] = nil
            continue
        }
        switch column.ti.TypeId {
        case typeNull:
            row[i] = nil
        case typeInt1:
            row[i] = buf[0]
        case typeBit:
            row[i] = buf[0] != 0
        case typeInt2:
            row[i] = int16(binary.LittleEndian.Uint16(buf))
        case typeInt4:
            row[i] = int32(binary.LittleEndian.Uint32(buf))
        case typeDateTim4:
            row[i] = decodeDateTim4(buf)
        case typeFlt4:
            row[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf))
        case typeMoney:
            row[i] = decodeMoney(buf)
        case typeDateTime:
            row[i] = decodeDateTime(buf)
        case typeFlt8:
            row[i] = math.Float64frombits(binary.LittleEndian.Uint64(buf))
        case typeMoney4:
            row[i] = decodeMoney4(buf)
        case typeInt8:
            row[i] = int64(binary.LittleEndian.Uint64(buf))
        case typeGuid:
            row[i] = decodeGuid(buf)
        case typeIntN:
            switch len(buf) {
            case 1:
                row[i] = uint8(buf[0])
            case 2:
                row[i] = int16(binary.LittleEndian.Uint16(buf))
            case 4:
                row[i] = int32(binary.LittleEndian.Uint32(buf))
            case 8:
                row[i] = int64(binary.LittleEndian.Uint64(buf))
            default:
                err = streamErrorf("Invalid size for INTNTYPE")
                return
            }
        case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
            row[i] = decodeDecimal(column.ti, buf)
        case typeBitN:
            if len(buf) != 1 {
                err = streamErrorf("Invalid size for BITNTYPE")
                return
            }
            row[i] = buf[0] != 0
        case typeFltN:
            switch len(buf) {
            case 4:
                row[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf))
            case 8:
                row[i] = math.Float64frombits(binary.LittleEndian.Uint64(buf))
            default:
                err = streamErrorf("Invalid size for FLTNTYPE")
                return
            }
        case typeMoneyN:
            switch len(buf) {
            case 4:
                row[i] = decodeMoney4(buf)
            case 8:
                row[i] = decodeMoney(buf)
            default:
                err = streamErrorf("Invalid size for MONEYNTYPE")
                return
            }
        case typeDateTimeN:
            switch len(buf) {
            case 4:
                row[i] = decodeDateTim4(buf)
            case 8:
                row[i] = decodeDateTime(buf)
            default:
                err = streamErrorf("Invalid size for DATETIMENTYPE")
                return
            }
        case typeDateN:
            if len(buf) != 3 {
                err = streamErrorf("Invalid size for DATENTYPE")
                return
            }
            row[i] = decodeDate(buf)
        case typeTimeN:
            row[i] = decodeTime(column.ti, buf)
        case typeDateTime2N:
            row[i] = decodeDateTime2(column.ti.Scale, buf)
        case typeDateTimeOffsetN:
            row[i] = decodeDateTimeOffset(column.ti.Scale, buf)
        case typeChar, typeVarChar, typeBigVarChar, typeBigChar, typeText:
            row[i] = decodeChar(column.ti, buf)
        case typeBinary, typeBigVarBin, typeBigBinary, typeImage:
            row[i] = buf
        case typeNVarChar, typeNChar, typeNText:
            row[i], err = decodeNChar(column, buf); if err != nil {
                return
            }
        case typeXml:
            row[i] = decodeXml(column, buf)
        case typeUdt:
            row[i] = decodeUdt(column, buf)
        default:
            panic("Invalid typeid")
        }
    }
    return row, nil
}


func parseError72(sess *tdsSession) (res Error, err error) {
    r := sess.buf
    hdr := struct {
        Length uint16
        Number int32
        State uint8
        Class uint8
    }{}
    err = binary.Read(r, binary.LittleEndian, &hdr)
    if err != nil {
        return
    }
    msgtext, err := readUsVarChar(r)
    if err != nil {
        return
    }
    servername, err := readBVarChar(r)
    if err != nil {
        return
    }
    procname, err := readBVarChar(r)
    if err != nil {
        return
    }
    var lineno int32
    err = binary.Read(r, binary.LittleEndian, &lineno)
    if err != nil {
        return
    }
    res = Error{
        Number: hdr.Number,
        State: hdr.State,
        Class: hdr.Class,
        Message: msgtext,
        ServerName: servername,
        ProcName: procname,
        LineNo: lineno,
    }
    return
}


func processResponse(sess *tdsSession, ch chan tokenStruct) (err error) {
    defer func() {
        if err := recover(); err != nil {
            ch <- err
        }
        close(ch)
    }()
    err = processResponseImpl(sess, ch)
    if err != nil {
        ch <- err
        return
    }
    return
}


func processResponseImpl(sess *tdsSession, ch chan tokenStruct) error {
    var packet_type uint8
    for {
        var timeout bool
        packet_type, timeout = sess.buf.BeginRead()
        if timeout {
            ch <- Error{timeout: true}
        } else {
            break
        }
    }
    if packet_type != packReply {
        return streamErrorf("invalid response packet type, expected REPLY, actual: %d", packet_type)
    }
    var columns []columnStruct
    for {
        token := sess.buf.byte()
        switch token {
        case tokenReturnStatus:
            returnStatus := parseReturnStatus(sess.buf)
            ch <- returnStatus
        case tokenLoginAck:
            loginAck := parseLoginAck(sess.buf)
            ch <- loginAck
        case tokenDoneInProc:
            done := parseDoneInProc(sess.buf)
            ch <- done
        case tokenDone, tokenDoneProc:
            done := parseDone(sess.buf)
            ch <- done
            if done.Status & doneMore == 0 {
                return nil
            }
        case tokenColMetadata:
            columns = parseColMetadata72(sess.buf)
            ch <- columns
        case tokenRow:
            row, err := parseRow(sess.buf, columns)
            if err != nil {
                return err
            }
            ch <- row
        case tokenEnvChange:
            if err := processEnvChg(sess); err != nil {
                return err
            }
        case tokenError:
            srverr, err := parseError72(sess)
            if err != nil {
                return err
            }
            sess.messages = append(sess.messages, srverr)
        default:
            return streamErrorf("Unknown token type: %d", token)
        }
    }
    return nil
}
