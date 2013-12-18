package main

import (
    "errors"
    "io"
    "fmt"
    "net"
    "os"
    iconv "github.com/djimenez/iconv-go"
    "strings"
    "strconv"
    "bufio"
    "encoding/binary"
)

var ascii2utf8 *iconv.Converter
var utf82ucs2 *iconv.Converter


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

type OutBuffer struct {
    buf []byte
    pos uint16
    transport io.Writer
}

func NewOutBuffer(bufsize int, transport io.Writer) *OutBuffer {
    buf := make([]byte, bufsize)
    w := new(OutBuffer)
    w.buf = buf
    w.pos = 8
    w.transport = transport
    return w
}

func (w * OutBuffer) Write(p []byte) (nn int, err error) {
    copied := copy(w.buf[w.pos:], p)
    w.pos += uint16(copied)
    return copied, nil
}

func (w * OutBuffer) WriteByte(b byte) error {
    w.buf[w.pos] = b
    w.pos += 1
    return nil
}

func (w * OutBuffer) BeginPacket(packet_type byte) {
    w.buf[0] = packet_type
    w.buf[1] = 0  // packet is incomplete
    w.pos = 8
}

func (w * OutBuffer) FinishPacket() error {
    w.buf[1] = 1  // packet is complete
    binary.BigEndian.PutUint16(w.buf[2:], w.pos)
    return WriteAll(w.transport, w.buf[:w.pos])
}

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

const VERSION = 0
const ENCRYPTION = 1
const INSTOPT = 2
const THREADID = 3
const MARS = 4
const TRACEID = 5
const TERMINATOR = 0xff


func WriteAll(w io.Writer, buf []byte) error {
    for len(buf) > 0 {
        written, err := w.Write(buf)
        if err != nil {
            return err
        }
        buf = buf[written:]
    }
    return nil
}


func WritePrelogin(w * OutBuffer, instance string) error {
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


func ReadPrelogin(r io.Reader) (map[uint8][]byte, error) {
    type Header struct {
        PacketType uint8
        Status uint8
        Size uint16
        Spid uint16
        PacketNo uint8
        Pad uint8
    }
    header := Header{}
    var err error
    err = binary.Read(r, binary.BigEndian, &header)
    if err != nil {
        return nil, err
    }
    if header.PacketType != 4 {
        return nil, errors.New("Invalid respones, expected packet type 4, PRELOGIN RESPONSE")
    }
    if header.Status != 1 {
        return nil, errors.New("Invalid respones, final packet")
    }
    struct_buf := make([]byte, header.Size - 8)
    read, err := r.Read(struct_buf)
    if err != nil {
        return nil, err
    }
    if read != len(struct_buf) {
        return nil, errors.New("Error invalid packet size")
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


func SendLogin(w * OutBuffer, login Login) error {
    w.BeginPacket(TDS7_LOGIN)
    hostname := str2ucs2(login.HostName)
    username := str2ucs2(login.UserName)
    password := str2ucs2(login.Password)
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
        HostNameLength: uint16(len(hostname)),
        UserNameLength: uint16(len(username)),
        PasswordLength: uint16(len(password)),
        AppNameLength: uint16(len(appname)),
        ServerNameLength: uint16(len(servername)),
        CtlIntNameLength: uint16(len(ctlintname)),
        LanguageLength: uint16(len(language)),
        DatabaseLength: uint16(len(database)),
        ClientID: login.ClientID,
        AtchDBFileLength: uint16(len(atchdbfile)),
        ChangePasswordLength: uint16(len(changepassword)),
    }
    offset := uint16(binary.Size(hdr))
    hdr.HostNameOffset = offset
    offset += hdr.HostNameLength
    hdr.UserNameOffset = offset
    offset += hdr.UserNameLength
    hdr.PasswordOffset = offset
    offset += hdr.PasswordLength
    hdr.AppNameOffset = offset
    offset += hdr.AppNameLength
    hdr.ServerNameOffset = offset
    offset += hdr.ServerNameLength
    hdr.CtlIntNameOffset = offset
    offset += hdr.CtlIntNameLength
    hdr.LanguageOffset = offset
    offset += hdr.LanguageLength
    hdr.DatabaseOffset = offset
    offset += hdr.DatabaseLength
    hdr.AtchDBFileOffset = offset
    offset += hdr.AtchDBFileLength
    hdr.ChangePasswordOffset = offset
    offset += hdr.ChangePasswordLength
    hdr.Length = uint32(offset)
    var err error
    err = binary.Write(w, binary.BigEndian, &hdr)
    if err != nil {
        return err
    }
    err = WriteAll(w, hostname)
    if err != nil {
        return err
    }
    err = WriteAll(w, username)
    if err != nil {
        return err
    }
    err = WriteAll(w, password)
    if err != nil {
        return err
    }
    err = WriteAll(w, appname)
    if err != nil {
        return err
    }
    err = WriteAll(w, servername)
    if err != nil {
        return err
    }
    err = WriteAll(w, ctlintname)
    if err != nil {
        return err
    }
    err = WriteAll(w, language)
    if err != nil {
        return err
    }
    err = WriteAll(w, database)
    if err != nil {
        return err
    }
    err = WriteAll(w, atchdbfile)
    if err != nil {
        return err
    }
    err = WriteAll(w, changepassword)
    if err != nil {
        return err
    }
    return nil
}


func main() {
    var err error
    ascii2utf8, err = iconv.NewConverter("ascii", "utf8")
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    utf82ucs2, err = iconv.NewConverter("utf8", "ucs2")
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    addr := os.Getenv("HOST")
    instance := os.Getenv("INSTANCE")
    var port uint64
    port = 1433
    if instance != "" {
        instances, err := get_instances(addr)
        if err != nil {
            fmt.Println("Error: ", err.Error())
            os.Exit(1)
        }
        fmt.Println("instances", instances)
        port, err = strconv.ParseUint(instances[instance]["tcp"], 0, 16)
        if err != nil {
            fmt.Println("Error: ", err.Error())
            os.Exit(1)
        }
    }
    conn, err := net.Dial("tcp", addr + ":" + strconv.FormatUint(port, 10))
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    fmt.Println(conn)

    outbuf := NewOutBuffer(1024, conn)
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

    r := bufio.NewReader(conn)
    prelogin, err := ReadPrelogin(r)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    for k, v := range prelogin {
        fmt.Println("rec", k, v)
    }

    login := Login{}
    err = SendLogin(outbuf, login)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
}
