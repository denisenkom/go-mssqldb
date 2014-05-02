package mssql

import (
	"encoding/binary"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
)

// token ids
const (
	tokenReturnStatus = 121 // 0x79
	tokenColMetadata  = 129 // 0x81
	tokenOrder        = 169 // 0xA9
	tokenError        = 170 // 0xAA
	tokenInfo         = 171 // 0xAB
	tokenLoginAck     = 173 // 0xad
	tokenRow          = 209 // 0xd1
	tokenNbcRow       = 210 // 0xd2
	tokenEnvChange    = 227 // 0xE3
	tokenDone         = 253 // 0xFD
	tokenDoneProc     = 254
	tokenDoneInProc   = 255
)

// done flags
const (
	doneFinal    = 0
	doneMore     = 1
	doneError    = 2
	doneInxact   = 4
	doneCount    = 0x10
	doneAttn     = 0x20
	doneSrvError = 0x100
)

// ENVCHANGE types
// http://msdn.microsoft.com/en-us/library/dd303449.aspx
const (
	envTypDatabase     = 1
	envTypLanguage     = 2
	envTypCharset      = 3
	envTypPacketSize   = 4
	envTypBeginTran    = 8
	envTypCommitTran   = 9
	envTypRollbackTran = 10
)

// interface for all tokens
type tokenStruct interface{}

type orderStruct struct {
	ColIds []uint16
}

type doneStruct struct {
	Status   uint16
	CurCmd   uint16
	RowCount uint64
}

type doneInProcStruct doneStruct

var doneFlags2str = map[uint16]string{
	doneFinal:    "final",
	doneMore:     "more",
	doneError:    "error",
	doneInxact:   "inxact",
	doneCount:    "count",
	doneAttn:     "attn",
	doneSrvError: "srverror",
}

func doneFlags2Str(flags uint16) string {
	strs := make([]string, 0, len(doneFlags2str))
	for flag, tag := range doneFlags2str {
		if flags&flag != 0 {
			strs = append(strs, tag)
		}
	}
	return strings.Join(strs, "|")
}

// ENVCHANGE stream
// http://msdn.microsoft.com/en-us/library/dd303449.aspx
func processEnvChg(sess *tdsSession) {
	size := sess.buf.uint16()
	r := &io.LimitedReader{R: sess.buf, N: int64(size)}
	for {
		var err error
		var envtype uint8
		err = binary.Read(r, binary.LittleEndian, &envtype)
		if err == io.EOF {
			return
		}
		if err != nil {
			badStreamPanic(err)
		}
		switch envtype {
		case envTypDatabase:
			_, err = readBVarChar(r)
			if err != nil {
				badStreamPanic(err)
			}
			sess.database, err = readBVarChar(r)
			if err != nil {
				badStreamPanic(err)
			}
		case envTypPacketSize:
			packetsize, err := readBVarChar(r)
			if err != nil {
				badStreamPanic(err)
			}
			_, err = readBVarChar(r)
			if err != nil {
				badStreamPanic(err)
			}
			packetsizei, err := strconv.Atoi(packetsize)
			if err != nil {
				badStreamPanicf("Invalid Packet size value returned from server (%s): %s", packetsize, err.Error())
			}
			if len(sess.buf.buf) != packetsizei {
				newbuf := make([]byte, packetsizei)
				copy(newbuf, sess.buf.buf)
				sess.buf.buf = newbuf
			}
		case envTypBeginTran:
			tranid, err := readBVarByte(r)
			if len(tranid) != 8 {
				badStreamPanicf("invalid size of transaction identifier: %d", len(tranid))
			}
			sess.tranid = binary.LittleEndian.Uint64(tranid)
			if err != nil {
				badStreamPanic(err)
			}
			_, err = readBVarByte(r)
			if err != nil {
				badStreamPanic(err)
			}
		case envTypCommitTran, envTypRollbackTran:
			_, err = readBVarByte(r)
			if err != nil {
				badStreamPanic(err)
			}
			_, err = readBVarByte(r)
			if err != nil {
				badStreamPanic(err)
			}
			sess.tranid = 0
		default:
			badStreamPanicf("unknown env type: %d", envtype)
		}

	}
}

type returnStatus int32

// http://msdn.microsoft.com/en-us/library/dd358180.aspx
func parseReturnStatus(r *tdsBuffer) returnStatus {
	return returnStatus(r.int32())
}

func parseOrder(r *tdsBuffer) (res orderStruct) {
	len := int(r.uint16())
	res.ColIds = make([]uint16, len/2)
	for i := 0; i < len/2; i++ {
		res.ColIds[i] = r.uint16()
	}
	return res
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
	Interface  uint8
	TDSVersion uint32
	ProgName   string
	ProgVer    uint32
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
	if res.ProgName, err = ucs22str(buf[1+4+1 : 1+4+1+prognamelen*2]); err != nil {
		badStreamPanic(err)
	}
	res.ProgVer = binary.BigEndian.Uint32(buf[size-4:])
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

func decodeVal(buf []byte, ti typeInfo) (res interface{}) {
	switch ti.TypeId {
	case typeNull:
		return nil
	case typeInt1:
		return int64(buf[0])
	case typeBit:
		return buf[0] != 0
	case typeInt2:
		return int64(int16(binary.LittleEndian.Uint16(buf)))
	case typeInt4:
		return int64(int32(binary.LittleEndian.Uint32(buf)))
	case typeDateTim4:
		return decodeDateTim4(buf)
	case typeFlt4:
		return math.Float32frombits(binary.LittleEndian.Uint32(buf))
	case typeMoney:
		return decodeMoney(buf)
	case typeDateTime:
		return decodeDateTime(buf)
	case typeFlt8:
		return math.Float64frombits(binary.LittleEndian.Uint64(buf))
	case typeMoney4:
		return decodeMoney4(buf)
	case typeInt8:
		return int64(binary.LittleEndian.Uint64(buf))
	case typeGuid:
		return decodeGuid(buf)
	case typeIntN:
		switch len(buf) {
		case 1:
			return int64(buf[0])
		case 2:
			return int64(int16((binary.LittleEndian.Uint16(buf))))
		case 4:
			return int64(int32(binary.LittleEndian.Uint32(buf)))
		case 8:
			return int64(binary.LittleEndian.Uint64(buf))
		default:
			badStreamPanicf("Invalid size for INTNTYPE")
		}
	case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
		return decodeDecimal(ti, buf)
	case typeBitN:
		if len(buf) != 1 {
			badStreamPanicf("Invalid size for BITNTYPE")
		}
		return buf[0] != 0
	case typeFltN:
		switch len(buf) {
		case 4:
			return float64(math.Float32frombits(binary.LittleEndian.Uint32(buf)))
		case 8:
			return math.Float64frombits(binary.LittleEndian.Uint64(buf))
		default:
			badStreamPanicf("Invalid size for FLTNTYPE")
		}
	case typeMoneyN:
		switch len(buf) {
		case 4:
			return decodeMoney4(buf)
		case 8:
			return decodeMoney(buf)
		default:
			badStreamPanicf("Invalid size for MONEYNTYPE")
		}
	case typeDateTimeN:
		switch len(buf) {
		case 4:
			return decodeDateTim4(buf)
		case 8:
			return decodeDateTime(buf)
		default:
			badStreamPanicf("Invalid size for DATETIMENTYPE")
		}
	case typeDateN:
		if len(buf) != 3 {
			badStreamPanicf("Invalid size for DATENTYPE")
		}
		return decodeDate(buf)
	case typeTimeN:
		return decodeTime(ti, buf)
	case typeDateTime2N:
		return decodeDateTime2(ti.Scale, buf)
	case typeDateTimeOffsetN:
		return decodeDateTimeOffset(ti.Scale, buf)
	case typeChar, typeVarChar, typeBigVarChar, typeBigChar, typeText:
		return decodeChar(ti, buf)
	case typeBinary, typeBigVarBin, typeBigBinary, typeImage:
		return buf
	case typeNVarChar, typeNChar, typeNText:
		return decodeNChar(ti, buf)
	case typeXml:
		return decodeXml(ti, buf)
	case typeUdt:
		return decodeUdt(ti, buf)
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

// http://msdn.microsoft.com/en-us/library/dd357254.aspx
func parseRow(r *tdsBuffer, columns []columnStruct, row []interface{}) {
	for i, column := range columns {
		var buf []byte
		buf = column.ti.Reader(&column.ti, r)
		if buf == nil {
			row[i] = nil
			continue
		}
		row[i] = decodeVal(buf, column.ti)
	}
}

// http://msdn.microsoft.com/en-us/library/dd304783.aspx
func parseNbcRow(r *tdsBuffer, columns []columnStruct, row []interface{}) {
	bitlen := (len(columns) + 7) / 8
	pres := make([]byte, bitlen)
	r.ReadFull(pres)
	for i, col := range columns {
		if pres[i/8]&(1<<(uint(i)%8)) != 0 {
			row[i] = nil
			continue
		}
		buf := col.ti.Reader(&col.ti, r)
		if buf == nil {
			row[i] = nil
			continue
		}
		row[i] = decodeVal(buf, col.ti)
	}
}

// http://msdn.microsoft.com/en-us/library/dd304156.aspx
func parseError72(r *tdsBuffer) (res Error) {
	length := r.uint16()
	_ = length // ignore length
	res.Number = r.int32()
	res.State = r.byte()
	res.Class = r.byte()
	res.Message = r.UsVarChar()
	res.ServerName = r.BVarChar()
	res.ProcName = r.BVarChar()
	res.LineNo = r.int32()
	return
}

// http://msdn.microsoft.com/en-us/library/dd304156.aspx
func parseInfo(r *tdsBuffer) (res Error) {
	length := r.uint16()
	_ = length // ignore length
	res.Number = r.int32()
	res.State = r.byte()
	res.Class = r.byte()
	res.Message = r.UsVarChar()
	res.ServerName = r.BVarChar()
	res.ProcName = r.BVarChar()
	res.LineNo = r.int32()
	return
}

func processResponse(sess *tdsSession, ch chan tokenStruct) (err error) {
	defer func() {
		if err := recover(); err != nil {
			switch err := err.(type) {
			case StreamError:
				ch <- err
			case net.Error:
				ch <- err
			default:
				panic(err)
			}
			ch <- err
		}
		close(ch)
	}()
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
	errors := make([]Error, 0, 10)
	messages := make([]Error, 0, 10)
	for {
		token := sess.buf.byte()
		switch token {
		case tokenReturnStatus:
			returnStatus := parseReturnStatus(sess.buf)
			ch <- returnStatus
		case tokenLoginAck:
			loginAck := parseLoginAck(sess.buf)
			ch <- loginAck
		case tokenOrder:
			order := parseOrder(sess.buf)
			ch <- order
		case tokenDoneInProc:
			done := parseDoneInProc(sess.buf)
			ch <- done
		case tokenDone, tokenDoneProc:
			done := parseDone(sess.buf)
			if done.Status&doneError != 0 {
				err = errors[len(errors)-1]
				ch <- err
				return err
			}
			ch <- done
			if done.Status&doneMore == 0 {
				return nil
			}
		case tokenColMetadata:
			columns = parseColMetadata72(sess.buf)
			ch <- columns
		case tokenRow:
			row := make([]interface{}, len(columns))
			parseRow(sess.buf, columns, row)
			ch <- row
		case tokenNbcRow:
			row := make([]interface{}, len(columns))
			parseNbcRow(sess.buf, columns, row)
			ch <- row
		case tokenEnvChange:
			processEnvChg(sess)
		case tokenError:
			srverr := parseError72(sess.buf)
			errors = append(errors, srverr)
		case tokenInfo:
			info := parseInfo(sess.buf)
			messages = append(messages, info)
		default:
			badStreamPanicf("Unknown token type: %d", token)
		}
	}
}
