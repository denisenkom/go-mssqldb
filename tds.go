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
}

func NewOutBuffer(bufsize int) *OutBuffer {
    buf := make([]byte, bufsize)
    w := new(OutBuffer)
    w.buf = buf
    w.pos = 8
    return w
}

func (w * OutBuffer) Write(p []byte) (nn int, err error) {
    copied := copy(w.buf[w.pos:], p)
    w.pos += uint16(copied)
    return copied, nil
}

func (w * OutBuffer) SetPacketType(b byte) {
    w.buf[0] = b
}

func (w * OutBuffer) BeginPacket(packet_type byte) {
    w.buf[0] = b
    w.pos = 8
}

const TDS71_PRELOGIN = 18
const VERSION = 0
const ENCRYPTION = 1
const INSTOPT = 2
const THREADID = 3
const MARS = 4
const TRACEID = 5
const TERMINATOR = 0xff


func WritePrelogin(outbuf * OutBuffer, instance string) error {
    var err error

    w := bufio.NewWriter(outbuf)

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

    outbuf.SetPacketType(TDS71_PRELOGIN)
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
    err = w.Flush()

    outbuf.buf[1] = 1  // packet is complete
    binary.BigEndian.PutUint16(outbuf.buf[2:], outbuf.pos)
    return nil
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


func SendLogin(w * OutBuffer) {
    w.BeginPacket(TDS7_LOGIN)
    type Header struct {
        Length uint32
        
    }
}


func main() {
    var err error
    ascii2utf8, err = iconv.NewConverter("ascii", "utf8")
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

    outbuf := NewOutBuffer(1024)
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
    written, err := conn.Write(outbuf.buf)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    if written != len(outbuf.buf) {
        fmt.Println("Error Write method didn't write the whole value")
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

    SendLogin()
}
