package mssql

import (
    "errors"
    "io"
    "fmt"
    "net"
    iconv "github.com/djimenez/iconv-go"
    "strings"
    "strconv"
    "encoding/binary"
    "io/ioutil"
    "unicode/utf8"
    "math"
)

var ascii2utf8 *iconv.Converter
var utf82ucs2 *iconv.Converter
var ucs22utf8 *iconv.Converter


func _parse_instances(msg []byte) (map[string]map[string]string) {
    results := map[string]map[string]string{}
    if len(msg) > 3 && msg[0] == 5 {
        var out = make([]byte, len(msg[3:]))
        var _, written, err = ascii2utf8.Convert(msg[3:], out)
        if err != nil {
            return results
        }
        out_s := string(out[:written])
        tokens := strings.Split(out_s, ";")
        instdict := map[string]string{}
        got_name := false
        var name string
        for _, token := range tokens {
            if got_name {
                instdict[name] = token
                got_name = false
            } else {
                name = token
                if len(name) == 0 {
                    if len(instdict) == 0 {
                        break
                    }
                    results[instdict["InstanceName"]] = instdict
                    instdict = map[string]string{}
                    continue
                }
                got_name = true
            }
        }
    }
    return results
}

func get_instances(address string) (map[string]map[string]string, error) {
    conn, err := net.Dial("udp", address + ":1434")
    if err != nil {
        return nil, err
    }
    _, err = conn.Write([]byte{3})
    if err != nil {
        return nil, err
    }
    var resp = make([]byte, 16 * 1024 - 1)
    read, err := conn.Read(resp)
    if err != nil {
        return nil, err
    }
    return _parse_instances(resp[:read]), nil
}

const TDS70 = 0x70000000
const TDS71 = 0x71000000
const TDS71rev1 = 0x71000001
const TDS72 = 0x72090002
const TDS73A = 0x730A0003
const TDS73 = TDS73A
const TDS73B = 0x730B0003
const TDS74 = 0x74000004

const TDS_QUERY = 1
const TDS_LOGIN = 2
const TDS_RPC = 3
const TDS_REPLY = 4
const TDS_CANCEL = 6
const TDS_BULK = 7
const TDS7_TRANS = 14
const TDS_NORMAL = 15
const TDS7_LOGIN = 16
const TDS7_AUTH = 17
const TDS71_PRELOGIN = 18

const (
    tokenColMetadata = 129  // 0x81
    TDS_ERROR_TOKEN = 170  // 0xAA
    TDS_LOGINACK_TOKEN = 173  // 0xad
    tokenRow = 209  // 0xd1
    TDS_ENVCHANGE_TOKEN = 227  // 0xE3
    TDS_DONE_TOKEN = 253  // 0xFD
    )

const VERSION = 0
const ENCRYPTION = 1
const INSTOPT = 2
const THREADID = 3
const MARS = 4
const TRACEID = 5
const TERMINATOR = 0xff

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

type tokenFunc func(*TdsSession, uint8, io.Reader) error

type TdsSession struct {
    buf * TdsBuffer

    loginAck loginAckStruct

    messages []Error

    tokenMap map[uint8]tokenFunc

    database string

    gotColumns bool
    responseStarted bool
    columns []columnStruct

    lastRow []interface{}
}


type doneStruct struct {
    Status uint16
    CurCmd uint16
    RowCount uint64
}


type columnStruct struct {
    UserType uint32
    Flags uint16
    ColName string
    TypeId uint8
    Size int
    Buffer []byte  // preallocated buffer for values
    Scale uint8
    Prec uint8
    Collation collation
    Reader func(column *columnStruct, r io.Reader) (res []byte, err error)
}


func streamErrorf(format string, v ...interface{}) error {
    return errors.New("Invalid TDS stream: " + fmt.Sprintf(format, v...))
}


func WritePrelogin(w * TdsBuffer, instance string) error {
    var err error

    instance_buf := make([]byte, len(instance))
    iconv.Convert([]byte(instance), instance_buf, "utf8", "ascii")
    instance_buf = append(instance_buf, 0)  // zero terminate instance name

    fields := map[uint8][]byte{
        VERSION: {0, 0, 0, 0, 0, 0},
        ENCRYPTION: {2},  // encryption not supported
        INSTOPT: instance_buf,
        THREADID: {0, 0, 0, 0},
        MARS: {0},  // MARS disabled
        }

    w.BeginPacket(TDS71_PRELOGIN)
    offset := uint16(5 * len(fields) + 1)
    // writing header
    for k, v := range fields {
        err = w.WriteByte(k)
        if err != nil {
            return err
        }
        size := uint16(len(v))
        err = binary.Write(w, binary.BigEndian, &offset)
        if err != nil {
            return err
        }
        err = binary.Write(w, binary.BigEndian, &size)
        if err != nil {
            return err
        }
        offset += size
    }
    err = w.WriteByte(TERMINATOR)
    if err != nil {
        return err
    }
    // writing values
    for _, v := range fields {
        written, err := w.Write(v)
        if err != nil {
            return err
        }
        if written != len(v) {
            return errors.New("Write method didn't write the whole value")
        }
    }
    return w.FinishPacket()
}


type Header struct {
    PacketType uint8
    Status uint8
    Size uint16
    Spid uint16
    PacketNo uint8
    Pad uint8
}


func ReadPrelogin(r * TdsBuffer) (map[uint8][]byte, error) {
    var err error
    packet_type, err := r.BeginRead()
    if err != nil {
        return nil, err
    }
    struct_buf, err := ioutil.ReadAll(r)
    if err != nil {
        return nil, err
    }
    if packet_type != 4 {
        return nil, errors.New("Invalid respones, expected packet type 4, PRELOGIN RESPONSE")
    }
    offset := 0
    results := map[uint8][]byte{}
    for true {
        rec_type := struct_buf[offset]
        if rec_type == TERMINATOR {
            break
        }

        rec_offset := binary.BigEndian.Uint16(struct_buf[offset + 1:])
        rec_len := binary.BigEndian.Uint16(struct_buf[offset + 3:])
        value := struct_buf[rec_offset:rec_offset + rec_len]
        results[rec_type] = value
        offset += 5
    }
    return results, nil
}


type Login struct {
    TDSVersion uint32
    PacketSize uint32
    ClientProgVer uint32
    ClientPID uint32
    ConnectionID uint32
    OptionFlags1 uint8
    OptionFlags2 uint8
    TypeFlags uint8
    OptionFlags3 uint8
    ClientTimeZone int32
    ClientLCID uint32
    HostName string
    UserName string
    Password string
    AppName string
    ServerName string
    CtlIntName string
    Language string
    Database string
    ClientID [6]byte
    SSPI []byte
    AtchDBFile string
    ChangePassword string
}


type LoginHeader struct {
    Length uint32
    TDSVersion uint32
    PacketSize uint32
    ClientProgVer uint32
    ClientPID uint32
    ConnectionID uint32
    OptionFlags1 uint8
    OptionFlags2 uint8
    TypeFlags uint8
    OptionFlags3 uint8
    ClientTimeZone int32
    ClientLCID uint32
    HostNameOffset uint16
    HostNameLength uint16
    UserNameOffset uint16
    UserNameLength uint16
    PasswordOffset uint16
    PasswordLength uint16
    AppNameOffset uint16
    AppNameLength uint16
    ServerNameOffset uint16
    ServerNameLength uint16
    ExtensionOffset uint16
    ExtensionLenght uint16
    CtlIntNameOffset uint16
    CtlIntNameLength uint16
    LanguageOffset uint16
    LanguageLength uint16
    DatabaseOffset uint16
    DatabaseLength uint16
    ClientID [6]byte
    SSPIOffset uint16
    SSPILength uint16
    AtchDBFileOffset uint16
    AtchDBFileLength uint16
    ChangePasswordOffset uint16
    ChangePasswordLength uint16
    SSPILongLength uint32
}


func str2ucs2(s string) []byte {
    res, err := utf82ucs2.ConvertString(s)
    if err != nil {
        panic("ConvertString failed unexpectedly: " + err.Error())
    }
    return []byte(res)
}


func ucs22str(s []byte) string {
    res, err := ucs22utf8.ConvertString(string(s))
    if err != nil {
        panic("ConvertString failed unexpectedly: " + err.Error())
    }
    return res
}


func manglePassword(password string) []byte {
    var ucs2password []byte = str2ucs2(password)
    for i, ch := range ucs2password {
        ucs2password[i] = ((ch << 4) & 0xff | (ch >> 4)) ^ 0xA5
    }
    return ucs2password
}


func SendLogin(w * TdsBuffer, login Login) error {
    w.BeginPacket(TDS7_LOGIN)
    hostname := str2ucs2(login.HostName)
    username := str2ucs2(login.UserName)
    password := manglePassword(login.Password)
    appname := str2ucs2(login.AppName)
    servername := str2ucs2(login.ServerName)
    ctlintname := str2ucs2(login.CtlIntName)
    language := str2ucs2(login.Language)
    database := str2ucs2(login.Database)
    atchdbfile := str2ucs2(login.AtchDBFile)
    changepassword := str2ucs2(login.ChangePassword)
    hdr := LoginHeader{
        TDSVersion: login.TDSVersion,
        PacketSize: login.PacketSize,
        ClientProgVer: login.ClientProgVer,
        ClientPID: login.ClientPID,
        ConnectionID: login.ConnectionID,
        OptionFlags1: login.OptionFlags1,
        OptionFlags2: login.OptionFlags2,
        TypeFlags: login.TypeFlags,
        OptionFlags3: login.OptionFlags3,
        ClientTimeZone: login.ClientTimeZone,
        ClientLCID: login.ClientLCID,
        HostNameLength: uint16(utf8.RuneCountInString(login.HostName)),
        UserNameLength: uint16(utf8.RuneCountInString(login.UserName)),
        PasswordLength: uint16(utf8.RuneCountInString(login.Password)),
        AppNameLength: uint16(utf8.RuneCountInString(login.AppName)),
        ServerNameLength: uint16(utf8.RuneCountInString(login.ServerName)),
        CtlIntNameLength: uint16(utf8.RuneCountInString(login.CtlIntName)),
        LanguageLength: uint16(utf8.RuneCountInString(login.Language)),
        DatabaseLength: uint16(utf8.RuneCountInString(login.Database)),
        ClientID: login.ClientID,
        SSPILength: uint16(len(login.SSPI)),
        AtchDBFileLength: uint16(utf8.RuneCountInString(login.AtchDBFile)),
        ChangePasswordLength: uint16(utf8.RuneCountInString(login.ChangePassword)),
    }
    offset := uint16(binary.Size(hdr))
    hdr.HostNameOffset = offset
    offset += uint16(len(hostname))
    hdr.UserNameOffset = offset
    offset += uint16(len(username))
    hdr.PasswordOffset = offset
    offset += uint16(len(password))
    hdr.AppNameOffset = offset
    offset += uint16(len(appname))
    hdr.ServerNameOffset = offset
    offset += uint16(len(servername))
    hdr.CtlIntNameOffset = offset
    offset += uint16(len(ctlintname))
    hdr.LanguageOffset = offset
    offset += uint16(len(language))
    hdr.DatabaseOffset = offset
    offset += uint16(len(database))
    hdr.SSPIOffset = offset
    offset += uint16(len(login.SSPI))
    hdr.AtchDBFileOffset = offset
    offset += uint16(len(atchdbfile))
    hdr.ChangePasswordOffset = offset
    offset += uint16(len(changepassword))
    hdr.Length = uint32(offset)
    var err error
    err = binary.Write(w, binary.LittleEndian, &hdr)
    if err != nil {
        return err
    }
    _, err = w.Write(hostname)
    if err != nil {
        return err
    }
    _, err = w.Write(username)
    if err != nil {
        return err
    }
    _, err = w.Write(password)
    if err != nil {
        return err
    }
    _, err = w.Write(appname)
    if err != nil {
        return err
    }
    _, err = w.Write(servername)
    if err != nil {
        return err
    }
    _, err = w.Write(ctlintname)
    if err != nil {
        return err
    }
    _, err = w.Write(language)
    if err != nil {
        return err
    }
    _, err = w.Write(database)
    if err != nil {
        return err
    }
    _, err = w.Write(atchdbfile)
    if err != nil {
        return err
    }
    _, err = w.Write(changepassword)
    if err != nil {
        return err
    }
    return w.FinishPacket()
}


const (
    envTypDatabase = 1
    envTypLanguage = 2
    envTypCharset = 3
    envTypPacketSize = 4
)


// ENVCHANGE stream
// http://msdn.microsoft.com/en-us/library/dd303449.aspx
func processEnvChg(sess *TdsSession, token uint8, r io.Reader) (err error) {
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
        fmt.Println("envtype", envtype)
        switch {
        case envtype == envTypDatabase:
            _, err = readBVarchar(r)
            if err != nil {
                return err
            }
            sess.database, err = readBVarchar(r)
            if err != nil {
                return err
            }
        case envtype == envTypPacketSize:
            _, err := readBVarchar(r)
            if err != nil {
                return err
            }
            packetsize, err := readBVarchar(r)
            if err != nil {
                return err
            }
            fmt.Println("packetsize", packetsize)
        default:
            return streamErrorf("unknown env type: %d", envtype)
        }

    }
    return nil
}


func parseDone(r io.Reader) (res doneStruct, err error) {
    err = binary.Read(r, binary.LittleEndian, &res)
    return res, err
}


type loginAckStruct struct {
    Interface uint8
    TDSVersion uint32
    ProgName string
    ProgVer uint32
}

func parseLoginAck(r io.Reader) (res loginAckStruct, err error) {
    var size uint16
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return
    }
    buf := make([]byte, size)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return
    }
    res.Interface = buf[0]
    res.TDSVersion = binary.BigEndian.Uint32(buf[1:])
    prognamelen := buf[1+4]
    res.ProgName = ucs22str(buf[1+4+1:1+4+1 + prognamelen * 2])
    res.ProgVer = binary.BigEndian.Uint32(buf[size - 4:])
    return
}


// http://msdn.microsoft.com/en-us/library/dd357363.aspx
func parseColMetadata72(r io.Reader) (columns []columnStruct, err error) {
    var count uint16
    err = binary.Read(r, binary.LittleEndian, &count)
    if err != nil {
        return nil, err
    }
    if count == 0xffff {
        // no metadata is sent
        return nil, nil
    }
    columns = make([]columnStruct, count)
    for i := range columns {
        column := &columns[i]
        err = binary.Read(r, binary.LittleEndian, &column.UserType)
        if err != nil {
            return nil, err
        }
        err = binary.Read(r, binary.LittleEndian, &column.Flags)
        if err != nil {
            return nil, err
        }

        // parsing TYPE_INFO structure
        err = binary.Read(r, binary.LittleEndian, &column.TypeId)
        if err != nil {
            return nil, err
        }
        switch column.TypeId {
        case typeNull, typeInt1, typeBit, typeInt2, typeInt4, typeDateTim4,
             typeFlt4, typeMoney, typeDateTime, typeFlt8, typeMoney4, typeInt8:
            // those are fixed length types
            switch column.TypeId {
            case typeNull:
                column.Size = 0
            case typeInt1, typeBit:
                column.Size = 1
            case typeInt2:
                column.Size = 2
            case typeInt4, typeDateTim4, typeFlt4, typeMoney4:
                column.Size = 4
            case typeMoney, typeDateTime, typeFlt8, typeInt8:
                column.Size = 8
            }
            column.Buffer = make([]byte, column.Size)
            column.Reader = readFixedType
        default:  // all others are VARLENTYPE
            err = readVarLen(column, r); if err != nil {
                return nil, err
            }
        }
        column.ColName, err = readBVarchar(r)
        if err != nil {
            return nil, err
        }
    }
    return columns, nil
}


// http://msdn.microsoft.com/en-us/library/dd357254.aspx
func parseRow(r io.Reader, columns []columnStruct) (row []interface{}, err error) {
    row = make([]interface{}, len(columns))
    for i, column := range columns {
        var buf []byte
        buf, err = column.Reader(&column, r); if err != nil {
            return
        }
        if buf == nil {
            row[i] = nil
            continue
        }
        switch column.TypeId {
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
            row[i] = decodeDecimal(column, buf)
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
            row[i] = decodeTime(column, buf)
        case typeDateTime2N:
            row[i] = decodeDateTime2(column.Scale, buf)
        case typeDateTimeOffsetN:
            row[i] = decodeDateTimeOffset(column.Scale, buf)
        case typeChar, typeVarChar, typeBigVarChar, typeBigChar, typeText:
            row[i] = decodeChar(column, buf)
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


func readUcs2(r io.Reader, numchars int) (res string, err error) {
    buf := make([]byte, numchars * 2)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return "", err
    }
    _, err = ucs22utf8.ConvertString(string(buf))
    if err != nil {
        return "", err
    }
    return string(buf), err
}


func readUsVarchar(r io.Reader) (res string, err error) {
    var numchars uint16
    err = binary.Read(r, binary.LittleEndian, &numchars)
    if err != nil {
        return "", err
    }
    return readUcs2(r, int(numchars))
}


func readBVarchar(r io.Reader) (res string, err error) {
    var numchars uint8
    err = binary.Read(r, binary.LittleEndian, &numchars)
    if err != nil {
        return "", err
    }
    return readUcs2(r, int(numchars))
}


func writeBVarchar(w io.Writer, str string) (err error) {
    if len(str) > 255 {
        panic("Invalid size for B_VARBYTE string")
    }
    buf := []byte{uint8(len(str))}
    _, err = w.Write(buf); if err != nil {
        return
    }
    _, err = w.Write(str2ucs2(str))
    return
}


func processError72(sess *TdsSession, token uint8, r io.Reader) (err error) {
    hdr := struct {
        Length uint16
        Number int32
        State uint8
        Class uint8
    }{}
    err = binary.Read(r, binary.LittleEndian, &hdr)
    if err != nil {
        return err
    }
    msgtext, err := readUsVarchar(r)
    if err != nil {
        return err
    }
    servername, err := readBVarchar(r)
    if err != nil {
        return err
    }
    procname, err := readBVarchar(r)
    if err != nil {
        return err
    }
    var lineno int32
    err = binary.Read(r, binary.LittleEndian, &lineno)
    if err != nil {
        return err
    }
    newerror := Error{
        Number: hdr.Number,
        State: hdr.State,
        Class: hdr.Class,
        Message: msgtext,
        ServerName: servername,
        ProcName: procname,
        LineNo: lineno,
    }
    sess.messages = append(sess.messages, newerror)
    return nil
}


// Packet Data Stream Headers
// http://msdn.microsoft.com/en-us/library/dd304953.aspx
type headerStruct struct {
    hdrtype uint16
    data []byte
}


const (
    dataStmHdrQueryNotif = 1  // query notifications
    dataStmHdrTransDescr = 2  // MARS transaction descriptor (required)
    dataStmHdrTraceActivity = 3
)


// MARS Transaction Descriptor Header
// http://msdn.microsoft.com/en-us/library/dd340515.aspx
type transDescrHdr struct {
    transDescr uint64  // transaction descriptor returned from ENVCHANGE
    outstandingReqCnt uint32  // outstanding request count
}

func (hdr transDescrHdr)pack() (res []byte) {
    res = make([]byte, 8 + 4)
    binary.LittleEndian.PutUint64(res, hdr.transDescr)
    binary.LittleEndian.PutUint32(res[8:], hdr.outstandingReqCnt)
    return res
}


func writeAllHeaders(w io.Writer, headers []headerStruct) (err error) {
    // calculatint total length
    var totallen uint32 = 4
    for _, hdr := range headers {
        totallen += 4 + 2 + uint32(len(hdr.data))
    }
    // writing
    err = binary.Write(w, binary.LittleEndian, totallen)
    if err != nil {
        return err
    }
    for _, hdr := range headers {
        var headerlen uint32 = 4 + 2 + uint32(len(hdr.data))
        err = binary.Write(w, binary.LittleEndian, headerlen)
        if err != nil {
            return err
        }
        err = binary.Write(w, binary.LittleEndian, hdr.hdrtype)
        if err != nil {
            return err
        }
        _, err = w.Write(hdr.data)
        if err != nil {
            return err
        }
    }
    return nil
}


func sendSqlBatch72(buf *TdsBuffer,
                  sqltext string,
                  headers []headerStruct) (err error) {
    buf.BeginPacket(TDS_QUERY)

    writeAllHeaders(buf, headers)

    _, err = buf.Write(str2ucs2(sqltext))
    if err != nil {
        return err
    }
    return buf.FinishPacket()
}


const (
    tmGetDtcAddr = 0
    tmPropagateXact = 1
    tmBeginXact = 5
    tmPromoteXact = 6
    tmCommitXact = 7
    tmRollbackXact = 8
    tmSaveXact = 9
)


func sendBeginXact(buf *TdsBuffer, isolation uint8,
                   name string, headers []headerStruct) (err error) {
    buf.BeginPacket(TDS7_TRANS)
    writeAllHeaders(buf, headers)
    var rqtype uint16 = tmBeginXact
    err = binary.Write(buf, binary.LittleEndian, &rqtype); if err != nil {
        return
    }
    err = binary.Write(buf, binary.LittleEndian, &isolation); if err != nil {
        return
    }
    err = writeBVarchar(buf, name)
    return
}


func beginProcessResponse(sess *TdsSession) (err error) {
    packet_type, err := sess.buf.BeginRead()
    if err != nil {
        return err
    }
    if packet_type != TDS_REPLY {
        return fmt.Errorf("Error: invalid response packet type, expected REPLY, actual: %d", packet_type)
    }
    sess.responseStarted = true
    return nil
}


type tokenStruct interface{}


func processResponse(sess *TdsSession, ch chan tokenStruct) (err error) {
    err = beginProcessResponse(sess)
    if err != nil {
        ch <- err
        close(ch)
        return err
    }
    var columns []columnStruct
    for true {
        token, err := sess.buf.ReadByte()
        if err != nil {
            ch <- err
            close(ch)
            return err
        }
        switch {
        case token == TDS_LOGINACK_TOKEN:
            loginAck, err := parseLoginAck(sess.buf)
            if err != nil {
                ch <- err
                close(ch)
                return err
            }
            ch <- loginAck
        case token == TDS_DONE_TOKEN:
            done, err := parseDone(sess.buf)
            if err != nil {
                ch <- err
                close(ch)
                return err
            }
            ch <- done
            if done.Status & doneMore == 0 {
                close(ch)
                return nil
            }
        case token == tokenColMetadata:
            columns, err = parseColMetadata72(sess.buf)
            if err != nil {
                ch <- err
                close(ch)
                return err
            }
            ch <- columns
        case token == tokenRow:
            row, err := parseRow(sess.buf, columns)
            if err != nil {
                ch <- err
                close(ch)
                return err
            }
            ch <- row
        default:
            if sess.tokenMap[token] == nil {
                err = fmt.Errorf("Unknown token type: %d", token)
                ch <- err
                close(ch)
                return err
            }
            err = sess.tokenMap[token](sess, token, sess.buf)
            if err != nil {
                err = fmt.Errorf("Failed processing token %d: %s",
                                 token, err.Error())
                ch <- err
                close(ch)
                return err
            }
        }
    }
    return nil
}


func init() {
    var err error
    ascii2utf8, err = iconv.NewConverter("ascii", "utf8")
    if err != nil {
        panic("Can't create ascii to utf8 convertor: " + err.Error())
    }
    utf82ucs2, err = iconv.NewConverter("utf8", "ucs2")
    if err != nil {
        panic("Can't create utf8 to ucs2 convertor: " + err.Error())
    }
    ucs22utf8, err = iconv.NewConverter("ucs2", "utf8")
    if err != nil {
        panic("Can't create ucs2 to utf8 convertor: " + err.Error())
    }
}


func Connect(params map[string]string) (res *TdsSession, err error) {
    var port uint64
    server := params["server"]
    parts := strings.SplitN(server, "\\", 2)
    host := parts[0]
    var instance string
    if len(parts) > 1 {
        instance = parts[1]
    }
    user := params["user id"]
    password := params["password"]
    port = 1433
    if instance != "" {
        instances, err := get_instances(host)
        if err != nil {
            f := "Unable to get instances from Sql Server Browser on host %v: %v"
            err = fmt.Errorf(f, host, err.Error())
            return nil, err
        }
        strport := instances[instance]["tcp"]
        port, err = strconv.ParseUint(strport, 0, 16)
        if err != nil {
            f := "Invalid tcp port returned from Sql Server Browser '%v': %v"
            return nil, fmt.Errorf(f, strport, err.Error())
        }
    }
    addr := host + ":" + strconv.FormatUint(port, 10)
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        f := "Unable to open tcp connection with host '%v': %v"
        return nil, fmt.Errorf(f, addr, err.Error())
    }

    outbuf := NewTdsBuffer(1024, conn)
    sess := TdsSession{
        buf: outbuf,
        messages: make([]Error, 0, 20),
    }
    //buf := make([]byte, 1024)
    //data := buf[8:]
    //buf[0] = // type
    //status := 1
    //buf[1] = status
    //binary.BigEndian.PutUint16(buf[1:], status)

    err = WritePrelogin(outbuf, instance)
    if err != nil {
        return nil, err
    }

    _, err = ReadPrelogin(outbuf)
    if err != nil {
        return nil, err
    }

    login := Login{
        TDSVersion: TDS73,
        PacketSize: uint32(len(outbuf.buf)),
        UserName: user,
        Password: password,
    }
    err = SendLogin(outbuf, login)
    if err != nil {
        return nil, err
    }

    // processing login response
    tokchan := make(chan tokenStruct, 5)
    go processResponse(&sess, tokchan)
    sess.tokenMap = map[uint8]tokenFunc{
        TDS_ENVCHANGE_TOKEN: processEnvChg,
        TDS_ERROR_TOKEN: processError72,
    }
    success := false
    for tok := range tokchan {
        switch token := tok.(type) {
        case loginAckStruct:
            success = true
            sess.loginAck = token
        }
    }
    if !success {
        if len(sess.messages) > 0 {
            return nil, sess.messages[0]
        } else {
            return nil, fmt.Errorf("Login failed")
        }
    }
    return &sess, nil
}
