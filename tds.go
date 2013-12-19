package mssql

import (
    "errors"
    "io"
    "fmt"
    "net"
    "os"
    iconv "github.com/djimenez/iconv-go"
    "strings"
    "strconv"
    "encoding/binary"
    "io/ioutil"
    "unicode/utf8"
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
            fmt.Println("Error: ", err.Error())
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

const TDS_LOGINACK_TOKEN = 0xad
const TDS_ENVCHANGE_TOKEN = 227  // 0xE3
const TDS_DONE_TOKEN = 253  // 0xFD

const VERSION = 0
const ENCRYPTION = 1
const INSTOPT = 2
const THREADID = 3
const MARS = 4
const TRACEID = 5
const TERMINATOR = 0xff


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
    fmt.Println(struct_buf)
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


func processEnvChg(token uint8, r io.Reader) (err error) {
    var size uint16
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return err
    }
    buf := make([]byte, size)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return err
    }
    typ := buf[0]
    fmt.Println("processEnvChg type:", typ)
    return nil
}


func processDone72(token uint8, r io.Reader) (err error) {
    data := struct {
        Status uint16
        CurCmd uint16
        RowCount uint64
    }{}
    err = binary.Read(r, binary.LittleEndian, &data)
    if err != nil {
        return err
    }
    fmt.Println("processDone72", data.Status, data.CurCmd, data.RowCount)
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


func Connect(params map[string]string) (buf *TdsBuffer, err error) {
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
        fmt.Println("instances", instances)
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
    //buf := make([]byte, 1024)
    //data := buf[8:]
    //buf[0] = // type
    //status := 1
    //buf[1] = status
    //binary.BigEndian.PutUint16(buf[1:], status)

    err = WritePrelogin(outbuf, instance)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }

    prelogin, err := ReadPrelogin(outbuf)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    for k, v := range prelogin {
        fmt.Println("rec", k, v)
    }

    login := Login{
        TDSVersion: TDS73,
        PacketSize: uint32(len(outbuf.buf)),
        UserName: user,
        Password: password,
    }
    err = SendLogin(outbuf, login)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }

    // processing login response
    packet_type, err := outbuf.BeginRead()
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    if packet_type != TDS_REPLY {
        conn.Close()
        fmt.Println("Error: invalid response packet type, expected REPLY, actual: ", packet_type)
        os.Exit(1)
    }
    type tokenFunc func(uint8, io.Reader) error
    tokenMap := map[uint8]tokenFunc{
        TDS_ENVCHANGE_TOKEN: processEnvChg,
        TDS_DONE_TOKEN: processDone72,
    }
    for true {
        token, err := outbuf.ReadByte()
        if err != nil {
            fmt.Println("Error: ", err.Error())
            os.Exit(1)
        }
        if token == TDS_LOGINACK_TOKEN {
            var size uint16
            err = binary.Read(outbuf, binary.LittleEndian, &size)
            if err != nil {
                fmt.Println("Error: ", err.Error())
                os.Exit(1)
            }
            buf := make([]byte, size)
            _, err := io.ReadFull(outbuf, buf)
            if err != nil {
                fmt.Println("Error: ", err.Error())
                os.Exit(1)
            }
            iface := buf[0]
            tdsver := binary.BigEndian.Uint32(buf[1:])
            prognamelen := buf[1+4]
            progname := ucs22str(buf[1+4+1:1+4+1+prognamelen])
            progver := buf[size-4:]
            fmt.Println("login ack", iface, tdsver, progver, progname)
        } else {
            if tokenMap[token] == nil {
                fmt.Println("Unknown token type:", token)
                os.Exit(1)
            }
            err = tokenMap[token](token, outbuf)
            if err != nil {
                fmt.Println("Failed processing token", err.Error())
                os.Exit(1)
            }
            if token == TDS_DONE_TOKEN {
                break
            }
        }
    }
    return outbuf, nil
}
