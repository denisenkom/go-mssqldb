package mssql

import (
    "io"
    "encoding/binary"
)


type header struct {
    PacketType uint8
    Status uint8
    Size uint16
    Spid uint16
    PacketNo uint8
    Pad uint8
}

type tdsBuffer struct {
    buf []byte
    pos uint16
    transport io.ReadWriteCloser
    size uint16
    final bool
    packet_type uint8
}

func newTdsBuffer(bufsize int, transport io.ReadWriteCloser) *tdsBuffer {
    buf := make([]byte, bufsize)
    w := new(tdsBuffer)
    w.buf = buf
    w.pos = 8
    w.transport = transport
    w.size = 0
    return w
}

func (w * tdsBuffer) Write(p []byte) (nn int, err error) {
    copied := copy(w.buf[w.pos:], p)
    w.pos += uint16(copied)
    return copied, nil
}

func (w * tdsBuffer) WriteByte(b byte) error {
    w.buf[w.pos] = b
    w.pos += 1
    return nil
}

func (w * tdsBuffer) BeginPacket(packet_type byte) {
    w.buf[0] = packet_type
    w.buf[1] = 0  // packet is incomplete
    w.pos = 8
}

func (w * tdsBuffer) FinishPacket() (err error) {
    w.buf[1] = 1  // packet is complete
    binary.BigEndian.PutUint16(w.buf[2:], w.pos)
    _, err = w.transport.Write(w.buf[:w.pos])
    return err
}

func (r * tdsBuffer) read_next_packet() error {
    header := header{}
    var err error
    err = binary.Read(r.transport, binary.BigEndian, &header)
    if err != nil {
        return err
    }
    offset := uint16(binary.Size(header))
    _, err = io.ReadFull(r.transport, r.buf[offset:header.Size])
    if err != nil {
        return err
    }
    r.pos = offset
    r.size = header.Size
    r.final = header.Status != 0
    r.packet_type = header.PacketType
    return nil
}

func (r * tdsBuffer) BeginRead() (packet_type uint8, err error) {
    err = r.read_next_packet()
    return r.packet_type, err
}

func (r * tdsBuffer) ReadByte() (res byte, err error) {
    if r.pos == r.size {
        if r.final {
            return 0, io.EOF
        }
        err = r.read_next_packet()
        if err != nil {
            return 0, err
        }
    }
    res = r.buf[r.pos]
    r.pos++
    return res, nil
}

func (r * tdsBuffer) Read(buf []byte) (n int, err error) {
    if r.pos == r.size {
        if r.final {
            return 0, io.EOF
        }
        err = r.read_next_packet()
        if err != nil {
            return 0, err
        }
    }
    copied := copy(buf, r.buf[r.pos:r.size])
    r.pos += uint16(copied)
    return copied, nil
}
