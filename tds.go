package main

import (
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
    w := bufio.NewWriter(outbuf)
    //buf := make([]byte, 1024)
    //data := buf[8:]
    //buf[0] = // type
    //status := 1
    //buf[1] = status
    //binary.BigEndian.PutUint16(buf[1:], status)
    const START_POS = 26
    var pos uint16 = START_POS
    write_field := func (tag byte, size uint16) {
        w.WriteByte(tag)
        binary.Write(w, binary.BigEndian, &pos)
        binary.Write(w, binary.BigEndian, &size)
        pos += size
    }
    const TDS71_PRELOGIN = 18
    const VERSION = 0
    const ENCRYPTION = 1
    const INSTOPT = 2
    const THREADID = 3
    const MARS = 4
    const TRACEID = 5
    const TERMINATOR = 0xff

    instance_buf := make([]byte, len(instance))
    iconv.Convert([]byte(instance), instance_buf, "utf8", "ascii")
    instance_buf = append(instance_buf, 0)  // zero terminate instance name

    outbuf.SetPacketType(TDS71_PRELOGIN)
    write_field(VERSION, 6)
    write_field(ENCRYPTION, 1)
    write_field(INSTOPT, uint16(len(instance_buf)))
    write_field(THREADID, 4)
    write_field(MARS, 1)
    w.WriteByte(TERMINATOR)
    var version uint32 = 0
    var build uint16 = 0
    binary.Write(w, binary.BigEndian, &version)
    binary.Write(w, binary.BigEndian, &build)
    w.WriteByte(2)  // encryption not supported
    w.Write(instance_buf)
    w.WriteByte(0)  // zero terminate instance name
    var thread_id uint32 = 0
    binary.Write(w, binary.BigEndian, &thread_id)
    w.WriteByte(0)  // MARS disabled
    w.Flush()

    outbuf.buf[1] = 1  // packet is complete
    binary.BigEndian.PutUint16(outbuf.buf[2:], outbuf.pos)

    conn.Write(outbuf.buf)

    inbuf := [1024]byte{}
    read, err := conn.Read(inbuf[:100])
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    fmt.Println(inbuf[:read])
}
