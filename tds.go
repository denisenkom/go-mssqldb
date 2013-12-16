package main

import (
    "fmt"
    "net"
    "os"
    iconv "github.com/djimenez/iconv-go"
    "strings"
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

func main() {
    var err error
    ascii2utf8, err = iconv.NewConverter("ascii", "utf8")
    addr := "192.168.1.34"
    instances, err := get_instances(addr)
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    fmt.Println("instances", instances)
    conn, err := net.Dial("tcp", addr + ":" + instances["SQLEXPRESS"]["tcp"])
    if err != nil {
        fmt.Println("Error: ", err.Error())
        os.Exit(1)
    }
    fmt.Println(conn)
}
