package mssql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	errs "github.com/pkg/errors"
)

func parseInstances(msg []byte) map[string]map[string]string {
	results := map[string]map[string]string{}
	if len(msg) > 3 && msg[0] == 5 {
		out_s := string(msg[3:])
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
					results[strings.ToUpper(instdict["InstanceName"])] = instdict
					instdict = map[string]string{}
					continue
				}
				got_name = true
			}
		}
	}
	return results
}

func getInstances(ctx context.Context, d Dialer, address string) (map[string]map[string]string, error) {
	conn, err := d.DialContext(ctx, "udp", address+":1434")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	_, err = conn.Write([]byte{3})
	if err != nil {
		return nil, err
	}
	var resp = make([]byte, 16*1024-1)
	read, err := conn.Read(resp)
	if err != nil {
		return nil, err
	}
	return parseInstances(resp[:read]), nil
}

// tds versions
const (
	verTDS70     = 0x70000000
	verTDS71     = 0x71000000
	verTDS71rev1 = 0x71000001
	verTDS72     = 0x72090002
	verTDS73A    = 0x730A0003
	verTDS73     = verTDS73A
	verTDS73B    = 0x730B0003
	verTDS74     = 0x74000004
)

// packet types
// https://msdn.microsoft.com/en-us/library/dd304214.aspx
const (
	PackSQLBatch   packetType = 1
	PackRPCRequest            = 3
	PackReply                 = 4

	// 2.2.1.7 Attention: https://msdn.microsoft.com/en-us/library/dd341449.aspx
	// 4.19.2 Out-of-Band Attention Signal: https://msdn.microsoft.com/en-us/library/dd305167.aspx
	PackAttention = 6

	PackBulkLoadBCP = 7
	PackTransMgrReq = 14
	PackNormal      = 15
	PackLogin7      = 16
	PackSSPIMessage = 17
	PackPrelogin    = 18
)

// prelogin fields
// http://msdn.microsoft.com/en-us/library/dd357559.aspx
const (
	PreloginVERSION    = 0
	PreloginENCRYPTION = 1
	PreloginINSTOPT    = 2
	PreloginTHREADID   = 3
	PreloginMARS       = 4
	PreloginTRACEID    = 5
	PreloginTERMINATOR = 0xff
)

const (
	EncryptOff    = 0 // Encryption is available but off.
	EncryptOn     = 1 // Encryption is available and on.
	EncryptNotSup = 2 // Encryption is not available.
	EncryptReq    = 3 // Encryption is required.
)

type tdsSession struct {
	buf          *TdsBuffer
	loginAck     loginAckStruct
	database     string
	partner      string
	columns      []columnStruct
	tranid       uint64
	logFlags     uint64
	log          optionalLogger
	routedServer string
	routedPort   uint16
}

const (
	logErrors      = 1
	logMessages    = 2
	logRows        = 4
	logSQL         = 8
	logParams      = 16
	logTransaction = 32
	logDebug       = 64
)

type columnStruct struct {
	UserType uint32
	Flags    uint16
	ColName  string
	ti       typeInfo
}

type KeySlice []uint8

func (p KeySlice) Len() int           { return len(p) }
func (p KeySlice) Less(i, j int) bool { return p[i] < p[j] }
func (p KeySlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// http://msdn.microsoft.com/en-us/library/dd357559.aspx
func writePrelogin(w *TdsBuffer, fields map[uint8][]byte) error {
	return writePreloginWithPacketType(w, fields, PackPrelogin)
}

// WritePreloginResponse writes the prelogin reply to a buffer
func WritePreloginResponse(w io.ReadWriteCloser, fields map[uint8][]byte) error {
	return writePreloginWithPacketType(w, fields, PackReply)
}

// WritePreloginRequest writes the prelogin request to a buffer
func WritePreloginRequest(w io.ReadWriteCloser, fields map[uint8][]byte) error {
	return writePreloginWithPacketType(w, fields, PackPrelogin)
}

// writePreloginWithPacketType writes a prelogin packet with a specific packet type
//
// There are two cases in which this method is called.
// 1. called by outside code as just a io.ReadWriteCloser
// 2. called by internal code as *TdsBuffer
// For (2) it's efficient to avoid reallocating the *TdsBuffer by asserting on the type of the passed in value of _w
func writePreloginWithPacketType(
	_w io.ReadWriteCloser,
	fields map[uint8][]byte,
	packetTypeValue uint8,
) error {
	w := NewIdempotentDefaultTdsBuffer(_w)

	var err error
	w.BeginPacket(packetType(packetTypeValue), false)
	offset := uint16(5*len(fields) + 1)
	keys := make(KeySlice, 0, len(fields))
	for k, _ := range fields {
		keys = append(keys, k)
	}
	sort.Sort(keys)
	// writing header
	for _, k := range keys {
		err = w.WriteByte(k)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.BigEndian, offset)
		if err != nil {
			return err
		}
		v := fields[k]
		size := uint16(len(v))
		err = binary.Write(w, binary.BigEndian, size)
		if err != nil {
			return err
		}
		offset += size
	}
	err = w.WriteByte(PreloginTERMINATOR)
	if err != nil {
		return err
	}
	// writing values
	for _, k := range keys {
		v := fields[k]
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

// https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-network-packet-size-server-configuration-option
// Default packet size remains at 4096 bytes
const bufferSize uint16 = 4096

// NewIdempotentDefaultTdsBuffer creates TDS buffer using the default packet size and
// does not reallocate a TdsBuffer if the provided transport is a TdsBuffer
func NewIdempotentDefaultTdsBuffer(transport io.ReadWriteCloser) *TdsBuffer {
	buffer, ok := transport.(*TdsBuffer)
	if !ok {
		buffer = NewTdsBuffer(bufferSize, transport)
	}

	return buffer
}

// readPreloginWithPacketType reads a prelogin packet with an expected packet type
//
// There are two cases in which this method is called.
// 1. called by outside code as just a io.ReadWriteCloser
// 2. called by internal code as *TdsBuffer
// For (2) it's efficient to avoid reallocating the *TdsBuffer by asserting on the type of the passed in value of _r
func readPreloginWithPacketType(
	_r io.ReadWriteCloser,
	expectedPacketTypeValue uint8,
) (map[uint8][]byte, error) {
	r := NewIdempotentDefaultTdsBuffer(_r)

	packet_type, err := r.BeginRead()
	if err != nil {
		return nil, err
	}
	struct_buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if packet_type != packetType(expectedPacketTypeValue) {
		return nil, errors.New(fmt.Sprintf("Invalid response, expected packet type %d", expectedPacketTypeValue))
	}
	offset := 0
	results := map[uint8][]byte{}
	for true {
		rec_type := struct_buf[offset]
		if rec_type == PreloginTERMINATOR {
			break
		}

		rec_offset := binary.BigEndian.Uint16(struct_buf[offset+1:])
		rec_len := binary.BigEndian.Uint16(struct_buf[offset+3:])
		value := struct_buf[rec_offset : rec_offset+rec_len]
		results[rec_type] = value
		offset += 5
	}
	return results, nil
}

func readPrelogin(r *TdsBuffer) (map[uint8][]byte, error) {
	return readPreloginWithPacketType(r, PackReply)
}

// ReadPreloginRequest parses a pre-login request from a transport.
func ReadPreloginRequest(r io.ReadWriteCloser) (map[uint8][]byte, error) {
	return readPreloginWithPacketType(r, PackPrelogin)
}

// ReadPreloginResponse parses a pre-login response from a transport.
func ReadPreloginResponse(r io.ReadWriteCloser) (map[uint8][]byte, error) {
	return readPreloginWithPacketType(r, PackReply)
}

// OptionFlags2
// http://msdn.microsoft.com/en-us/library/dd304019.aspx
const (
	fLanguageFatal = 1
	fODBC          = 2
	fTransBoundary = 4
	fCacheConnect  = 8
	fIntSecurity   = 0x80
)

// TypeFlags
const (
	// 4 bits for fSQLType
	// 1 bit for fOLEDB
	fReadOnlyIntent = 32
)

type login struct {
	TDSVersion     uint32
	PacketSize     uint32
	ClientProgVer  uint32
	ClientPID      uint32
	ConnectionID   uint32
	OptionFlags1   uint8
	OptionFlags2   uint8
	TypeFlags      uint8
	OptionFlags3   uint8
	ClientTimeZone int32
	ClientLCID     uint32
	HostName       string
	UserName       string
	Password       string
	AppName        string
	ServerName     string
	CtlIntName     string
	Language       string
	Database       string
	ClientID       [6]byte
	SSPI           []byte
	AtchDBFile     string
	ChangePassword string
}

type loginHeader struct {
	Length               uint32
	TDSVersion           uint32
	PacketSize           uint32
	ClientProgVer        uint32
	ClientPID            uint32
	ConnectionID         uint32
	OptionFlags1         uint8
	OptionFlags2         uint8
	TypeFlags            uint8
	OptionFlags3         uint8
	ClientTimeZone       int32
	ClientLCID           uint32
	HostNameOffset       uint16
	HostNameLength       uint16
	UserNameOffset       uint16
	UserNameLength       uint16
	PasswordOffset       uint16
	PasswordLength       uint16
	AppNameOffset        uint16
	AppNameLength        uint16
	ServerNameOffset     uint16
	ServerNameLength     uint16
	ExtensionOffset      uint16
	ExtensionLenght      uint16
	CtlIntNameOffset     uint16
	CtlIntNameLength     uint16
	LanguageOffset       uint16
	LanguageLength       uint16
	DatabaseOffset       uint16
	DatabaseLength       uint16
	ClientID             [6]byte
	SSPIOffset           uint16
	SSPILength           uint16
	AtchDBFileOffset     uint16
	AtchDBFileLength     uint16
	ChangePasswordOffset uint16
	ChangePasswordLength uint16
	SSPILongLength       uint32
}

// convert Go string to UTF-16 encoded []byte (littleEndian)
// done manually rather than using bytes and binary packages
// for performance reasons
func str2ucs2(s string) []byte {
	res := utf16.Encode([]rune(s))
	ucs2 := make([]byte, 2*len(res))
	for i := 0; i < len(res); i++ {
		ucs2[2*i] = byte(res[i])
		ucs2[2*i+1] = byte(res[i] >> 8)
	}
	return ucs2
}

func ucs22str(s []byte) (string, error) {
	if len(s)%2 != 0 {
		return "", fmt.Errorf("Illegal UCS2 string length: %d", len(s))
	}
	buf := make([]uint16, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		buf[i/2] = binary.LittleEndian.Uint16(s[i:])
	}
	return string(utf16.Decode(buf)), nil
}

func manglePassword(password string) []byte {
	var ucs2password []byte = str2ucs2(password)
	for i, ch := range ucs2password {
		ucs2password[i] = ((ch<<4)&0xff | (ch >> 4)) ^ 0xA5
	}
	return ucs2password
}

// WriteLoginRequest writes a login request via a TdsBuffer.
func WriteLoginRequest(_w io.ReadWriteCloser, login *LoginRequest) error {
	w := NewIdempotentDefaultTdsBuffer(_w)

	return sendLogin(w, login.login)
}

// http://msdn.microsoft.com/en-us/library/dd304019.aspx
func sendLogin(w *TdsBuffer, login login) error {
	w.BeginPacket(PackLogin7, false)
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
	hdr := loginHeader{
		TDSVersion:           login.TDSVersion,
		PacketSize:           login.PacketSize,
		ClientProgVer:        login.ClientProgVer,
		ClientPID:            login.ClientPID,
		ConnectionID:         login.ConnectionID,
		OptionFlags1:         login.OptionFlags1,
		OptionFlags2:         login.OptionFlags2,
		TypeFlags:            login.TypeFlags,
		OptionFlags3:         login.OptionFlags3,
		ClientTimeZone:       login.ClientTimeZone,
		ClientLCID:           login.ClientLCID,
		HostNameLength:       uint16(utf8.RuneCountInString(login.HostName)),
		UserNameLength:       uint16(utf8.RuneCountInString(login.UserName)),
		PasswordLength:       uint16(utf8.RuneCountInString(login.Password)),
		AppNameLength:        uint16(utf8.RuneCountInString(login.AppName)),
		ServerNameLength:     uint16(utf8.RuneCountInString(login.ServerName)),
		CtlIntNameLength:     uint16(utf8.RuneCountInString(login.CtlIntName)),
		LanguageLength:       uint16(utf8.RuneCountInString(login.Language)),
		DatabaseLength:       uint16(utf8.RuneCountInString(login.Database)),
		ClientID:             login.ClientID,
		SSPILength:           uint16(len(login.SSPI)),
		AtchDBFileLength:     uint16(utf8.RuneCountInString(login.AtchDBFile)),
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
	_, err = w.Write(login.SSPI)
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

// LoginRequest embeds login. This is a hack to expose login to the outside world while minimising
// changes to code.
type LoginRequest struct {
	login
}

// offsetAfterHeader calculates an offset adjusted for reading from a buffer that starts
// after the packet header.
func offsetAfterHeader(offset uint16) int {
	return headerSize + int(offset)
}

func readUcs2FromTds(
	r *TdsBuffer,
	numchars int,
	offset uint16,
) (res string, err error) {
	r.rpos = offsetAfterHeader(offset)
	return readUcs2(r, numchars)
}

// ReadLoginRequest parses a TDS7 login packet.
func ReadLoginRequest(_r io.ReadWriteCloser) (*LoginRequest, error) {
	r := NewIdempotentDefaultTdsBuffer(_r)
	var err error

	packet_type, err := r.BeginRead()
	if err != nil {
		return nil, err
	}

	if packet_type != PackLogin7 {
		return nil, errors.New(fmt.Sprintf("Invalid response, expected packet type %d", PackLogin7))
	}

	hdr := loginHeader{}

	err = binary.Read(r, binary.LittleEndian, &hdr)
	if err != nil {
		return nil, err
	}

	hostname, err := readUcs2FromTds(r, int(hdr.HostNameLength), hdr.HostNameOffset)
	if err != nil {
		return nil, err
	}

	username, err := readUcs2FromTds(r, int(hdr.UserNameLength), hdr.UserNameOffset)
	if err != nil {
		return nil, err
	}

	password, err := readUcs2FromTds(r, int(hdr.PasswordLength), hdr.PasswordOffset)
	if err != nil {
		return nil, err
	}

	appname, err := readUcs2FromTds(r, int(hdr.AppNameLength), hdr.AppNameOffset)
	if err != nil {
		return nil, err
	}

	servername, err := readUcs2FromTds(r, int(hdr.ServerNameLength), hdr.ServerNameOffset)
	if err != nil {
		return nil, err
	}

	ctlintname, err := readUcs2FromTds(r, int(hdr.CtlIntNameLength), hdr.CtlIntNameOffset)
	if err != nil {
		return nil, err
	}

	language, err := readUcs2FromTds(r, int(hdr.LanguageLength), hdr.LanguageOffset)
	if err != nil {
		return nil, err
	}

	database, err := readUcs2FromTds(r, int(hdr.DatabaseLength), hdr.DatabaseOffset)
	if err != nil {
		return nil, err
	}

	r.rpos = offsetAfterHeader(hdr.SSPIOffset)
	sspi := make([]byte, int(hdr.SSPILength))
	_, err = io.ReadFull(r, sspi)
	if err != nil {
		return nil, err
	}

	atchdbfile, err := readUcs2FromTds(r, int(hdr.AtchDBFileLength), hdr.AtchDBFileOffset)
	if err != nil {
		return nil, err
	}

	changepassword, err := readUcs2FromTds(
		r, int(hdr.ChangePasswordLength), hdr.ChangePasswordOffset,
	)
	if err != nil {
		return nil, err
	}

	return &LoginRequest{
		login: login{
			TDSVersion:     hdr.TDSVersion,
			PacketSize:     hdr.PacketSize,
			ClientProgVer:  hdr.ClientProgVer,
			ClientPID:      hdr.ClientPID,
			ConnectionID:   hdr.ConnectionID,
			OptionFlags1:   hdr.OptionFlags1,
			OptionFlags2:   hdr.OptionFlags2,
			TypeFlags:      hdr.TypeFlags,
			OptionFlags3:   hdr.OptionFlags3,
			ClientTimeZone: hdr.ClientTimeZone,
			ClientLCID:     hdr.ClientLCID,
			HostName:       hostname,
			UserName:       username,
			Password:       password,
			AppName:        appname,
			ServerName:     servername,
			CtlIntName:     ctlintname,
			Language:       language,
			Database:       database,
			ClientID:       hdr.ClientID,
			SSPI:           sspi,
			AtchDBFile:     atchdbfile,
			ChangePassword: changepassword,
		},
	}, nil
}

func recoverToError() error {
	var err error
	if r := recover(); r != nil {
		switch x := r.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("unknown panic")
		}
	}

	return err
}

// ReadLoginResponse parses a TDS7 login response packet.
func ReadLoginResponse(_r io.ReadWriteCloser) (l *LoginResponse, err error) {
	defer func() {
		if panicErr := recoverToError(); panicErr != nil {
			err = panicErr
		}
	}()

	r := NewIdempotentDefaultTdsBuffer(_r)

	_, err = r.BeginRead()
	if err != nil {
		return
	}

	r.byte()

	res := parseLoginAck(r)

	return &LoginResponse{loginAckStruct: res}, nil
}

// ReadError parses a TDS7 error packet.
func ReadError(_r io.ReadWriteCloser) (protocolErr *Error, err error) {
	defer func() {
		if panicErr := recoverToError(); panicErr != nil {
			err = panicErr
		}
	}()

	r := NewIdempotentDefaultTdsBuffer(_r)

	_, err = r.BeginRead()
	if err != nil {
		return
	}

	r.byte()

	res := parseError72(r)

	return &res, nil
}

func readUcs2(r io.Reader, numchars int) (res string, err error) {
	buf := make([]byte, numchars*2)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	return ucs22str(buf)
}

func readUsVarChar(r io.Reader) (res string, err error) {
	numchars, err := readUshort(r)
	if err != nil {
		return
	}
	return readUcs2(r, int(numchars))
}

func writeUsVarChar(w io.Writer, s string) (err error) {
	buf := str2ucs2(s)
	var numchars int = len(buf) / 2
	if numchars > 0xffff {
		panic("invalid size for US_VARCHAR")
	}
	err = binary.Write(w, binary.LittleEndian, uint16(numchars))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readBVarChar(r io.Reader) (res string, err error) {
	numchars, err := readByte(r)
	if err != nil {
		return "", err
	}

	// A zero length could be returned, return an empty string
	if numchars == 0 {
		return "", nil
	}
	return readUcs2(r, int(numchars))
}

func writeBVarChar(w io.Writer, s string) (err error) {
	buf := str2ucs2(s)
	var numchars int = len(buf) / 2
	if numchars > 0xff {
		panic("invalid size for B_VARCHAR")
	}
	err = binary.Write(w, binary.LittleEndian, uint8(numchars))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readBVarByte(r io.Reader) (res []byte, err error) {
	length, err := readByte(r)
	if err != nil {
		return
	}
	res = make([]byte, length)
	_, err = io.ReadFull(r, res)
	return
}

func readUshort(r io.Reader) (res uint16, err error) {
	err = binary.Read(r, binary.LittleEndian, &res)
	return
}

func readByte(r io.Reader) (res byte, err error) {
	var b [1]byte
	_, err = r.Read(b[:])
	res = b[0]
	return
}

// Packet Data Stream Headers
// http://msdn.microsoft.com/en-us/library/dd304953.aspx
type headerStruct struct {
	hdrtype uint16
	data    []byte
}

const (
	dataStmHdrQueryNotif    = 1 // query notifications
	dataStmHdrTransDescr    = 2 // MARS transaction descriptor (required)
	dataStmHdrTraceActivity = 3
)

// Query Notifications Header
// http://msdn.microsoft.com/en-us/library/dd304949.aspx
type queryNotifHdr struct {
	notifyId      string
	ssbDeployment string
	notifyTimeout uint32
}

func (hdr queryNotifHdr) pack() (res []byte) {
	notifyId := str2ucs2(hdr.notifyId)
	ssbDeployment := str2ucs2(hdr.ssbDeployment)

	res = make([]byte, 2+len(notifyId)+2+len(ssbDeployment)+4)
	b := res

	binary.LittleEndian.PutUint16(b, uint16(len(notifyId)))
	b = b[2:]
	copy(b, notifyId)
	b = b[len(notifyId):]

	binary.LittleEndian.PutUint16(b, uint16(len(ssbDeployment)))
	b = b[2:]
	copy(b, ssbDeployment)
	b = b[len(ssbDeployment):]

	binary.LittleEndian.PutUint32(b, hdr.notifyTimeout)

	return res
}

// MARS Transaction Descriptor Header
// http://msdn.microsoft.com/en-us/library/dd340515.aspx
type transDescrHdr struct {
	transDescr        uint64 // transaction descriptor returned from ENVCHANGE
	outstandingReqCnt uint32 // outstanding request count
}

func (hdr transDescrHdr) pack() (res []byte) {
	res = make([]byte, 8+4)
	binary.LittleEndian.PutUint64(res, hdr.transDescr)
	binary.LittleEndian.PutUint32(res[8:], hdr.outstandingReqCnt)
	return res
}

func writeAllHeaders(w io.Writer, headers []headerStruct) (err error) {
	// Calculating total length.
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

func sendSqlBatch72(buf *TdsBuffer, sqltext string, headers []headerStruct, resetSession bool) (err error) {
	buf.BeginPacket(PackSQLBatch, resetSession)

	if err = writeAllHeaders(buf, headers); err != nil {
		return
	}

	_, err = buf.Write(str2ucs2(sqltext))
	if err != nil {
		return
	}
	return buf.FinishPacket()
}

// 2.2.1.7 Attention: https://msdn.microsoft.com/en-us/library/dd341449.aspx
// 4.19.2 Out-of-Band Attention Signal: https://msdn.microsoft.com/en-us/library/dd305167.aspx
func sendAttention(buf *TdsBuffer) error {
	buf.BeginPacket(PackAttention, false)
	return buf.FinishPacket()
}

type auth interface {
	InitialBytes() ([]byte, error)
	NextBytes([]byte) ([]byte, error)
	Free()
}

// SQL Server AlwaysOn Availability Group Listeners are bound by DNS to a
// list of IP addresses.  So if there is more than one, try them all and
// use the first one that allows a connection.
func dialConnection(ctx context.Context, c *Connector, p connectParams) (conn net.Conn, err error) {
	var ips []net.IP
	ips, err = net.LookupIP(p.host)
	if err != nil {
		ip := net.ParseIP(p.host)
		if ip == nil {
			return nil, err
		}
		ips = []net.IP{ip}
	}
	if len(ips) == 1 {
		d := c.getDialer(&p)
		addr := net.JoinHostPort(ips[0].String(), strconv.Itoa(int(p.port)))
		conn, err = d.DialContext(ctx, "tcp", addr)

	} else {
		//Try Dials in parallel to avoid waiting for timeouts.
		connChan := make(chan net.Conn, len(ips))
		errChan := make(chan error, len(ips))
		portStr := strconv.Itoa(int(p.port))
		for _, ip := range ips {
			go func(ip net.IP) {
				d := c.getDialer(&p)
				addr := net.JoinHostPort(ip.String(), portStr)
				conn, err := d.DialContext(ctx, "tcp", addr)
				if err == nil {
					connChan <- conn
				} else {
					errChan <- err
				}
			}(ip)
		}
		// Wait for either the *first* successful connection, or all the errors
	wait_loop:
		for i, _ := range ips {
			select {
			case conn = <-connChan:
				// Got a connection to use, close any others
				go func(n int) {
					for i := 0; i < n; i++ {
						select {
						case conn := <-connChan:
							conn.Close()
						case <-errChan:
						}
					}
				}(len(ips) - i - 1)
				// Remove any earlier errors we may have collected
				err = nil
				break wait_loop
			case err = <-errChan:
			}
		}
	}
	// Can't do the usual err != nil check, as it is possible to have gotten an error before a successful connection
	if conn == nil {
		f := "Unable to open tcp connection with host '%v:%v': %v"
		return nil, fmt.Errorf(f, p.host, p.port, err.Error())
	}
	return conn, err
}

func connect(ctx context.Context, c *Connector, log optionalLogger, p connectParams) (res *tdsSession, err error) {
	dialCtx := ctx
	connectInterceptor := ConnectInterceptorFromContext(ctx)

	if p.dial_timeout > 0 {
		var cancel func()
		dialCtx, cancel = context.WithTimeout(ctx, p.dial_timeout)
		defer cancel()
	}
	// if instance is specified use instance resolution service
	if p.instance != "" {
		p.instance = strings.ToUpper(p.instance)
		d := c.getDialer(&p)
		instances, err := getInstances(dialCtx, d, p.host)
		if err != nil {
			f := "Unable to get instances from Sql Server Browser on host %v: %v"
			return nil, fmt.Errorf(f, p.host, err.Error())
		}
		strport, ok := instances[p.instance]["tcp"]
		if !ok {
			f := "No instance matching '%v' returned from host '%v'"
			return nil, fmt.Errorf(f, p.instance, p.host)
		}
		p.port, err = strconv.ParseUint(strport, 0, 16)
		if err != nil {
			f := "Invalid tcp port returned from Sql Server Browser '%v': %v"
			return nil, fmt.Errorf(f, strport, err.Error())
		}
	}

initiate_connection:
	conn, err := dialConnection(dialCtx, c, p)
	if err != nil {
		return nil, err
	}

	toconn := newTimeoutConn(conn, p.conn_timeout)

	outbuf := NewTdsBuffer(p.packetSize, toconn)
	sess := tdsSession{
		buf:      outbuf,
		log:      log,
		logFlags: p.logFlags,
	}

	instance_buf := []byte(p.instance)
	instance_buf = append(instance_buf, 0) // zero terminate instance name
	var encrypt byte
	if p.disableEncryption {
		encrypt = EncryptNotSup
	} else if p.encrypt {
		encrypt = EncryptOn
	} else {
		encrypt = EncryptOff
	}
	fields := map[uint8][]byte{
		PreloginVERSION:    {0, 0, 0, 0, 0, 0},
		PreloginENCRYPTION: {encrypt},
		PreloginINSTOPT:    instance_buf,
		PreloginTHREADID:   {0, 0, 0, 0},
		PreloginMARS:       {0}, // MARS disabled
	}

	err = writePrelogin(outbuf, fields)
	if err != nil {
		return nil, err
	}

	fields, err = readPrelogin(outbuf)
	if err != nil {
		return nil, err
	}

	encryptBytes, ok := fields[PreloginENCRYPTION]
	if !ok {
		return nil, fmt.Errorf("Encrypt negotiation failed")
	}
	encrypt = encryptBytes[0]
	if p.encrypt && (encrypt == EncryptNotSup || encrypt == EncryptOff) {
		return nil, fmt.Errorf("Server does not support encryption")
	}

	if encrypt != EncryptNotSup {
		var config tls.Config
		if p.certificate != "" {
			pem, err := ioutil.ReadFile(p.certificate)
			if err != nil {
				return nil, fmt.Errorf("Cannot read certificate %q: %v", p.certificate, err)
			}
			certs := x509.NewCertPool()
			certs.AppendCertsFromPEM(pem)
			config.RootCAs = certs
		}
		if p.trustServerCertificate {
			config.InsecureSkipVerify = true
		}
		config.ServerName = p.hostInCertificate
		// fix for https://github.com/denisenkom/go-mssqldb/issues/166
		// Go implementation of TLS payload size heuristic algorithm splits single TDS package to multiple TCP segments,
		// while SQL Server seems to expect one TCP segment per encrypted TDS package.
		// Setting DynamicRecordSizingDisabled to true disables that algorithm and uses 16384 bytes per TLS package
		config.DynamicRecordSizingDisabled = true
		// setting up connection handler which will allow wrapping of TLS handshake packets inside TDS stream
		handshakeConn := tlsHandshakeConn{buf: outbuf}
		passthrough := passthroughConn{c: &handshakeConn}
		tlsConn := tls.Client(&passthrough, &config)
		err = tlsConn.Handshake()
		passthrough.c = toconn
		outbuf.transport = tlsConn
		if err != nil {
			return nil, fmt.Errorf("TLS Handshake failed: %v", err)
		}
		if encrypt == EncryptOff {
			outbuf.afterFirst = func() {
				outbuf.transport = toconn
			}
		}
	}

	// Intercept prelogin response and send to secretless
	if connectInterceptor != nil && connectInterceptor.ServerPreLoginResponse != nil {
		connectInterceptor.ServerPreLoginResponse <- fields
	}

	// Initialise params
	Database := p.database
	HostName := p.workstation
	ServerName := p.host
	AppName := p.appname
	TypeFlags := p.typeFlags

	// Replaces params with values from the client LoginRequest
	if connectInterceptor != nil && connectInterceptor.ClientLoginRequest != nil {
		clientLoginRequest := <-connectInterceptor.ClientLoginRequest
		if clientLoginRequest == nil {
			return nil, errors.New("Login error: ClientLoginRequest is nil")
		}

		Database = clientLoginRequest.Database
		HostName = clientLoginRequest.HostName
		ServerName = clientLoginRequest.ServerName
		AppName = clientLoginRequest.AppName
		TypeFlags = clientLoginRequest.TypeFlags
	}

	login := login{
		TDSVersion:   verTDS74,
		PacketSize:   uint32(outbuf.PackageSize()),
		Database:     Database,
		OptionFlags2: fODBC, // to get unlimited TEXTSIZE
		HostName:     HostName,
		ServerName:   ServerName,
		AppName:      AppName,
		TypeFlags:    TypeFlags,
	}
	auth, auth_ok := getAuth(p.user, p.password, p.serverSPN, p.workstation)
	if auth_ok {
		login.SSPI, err = auth.InitialBytes()
		if err != nil {
			return nil, err
		}
		login.OptionFlags2 |= fIntSecurity
		defer auth.Free()
	} else {
		login.UserName = p.user
		login.Password = p.password
	}
	err = sendLogin(outbuf, login)
	if err != nil {
		return nil, err
	}

	// If this is in the context of secretless broker, then we can call it
	// a day after sending the client login request to the server.
	// The rest of the negotiation takes place out of this method (i.e.
	// transparently, within a duplex pipe)
	if connectInterceptor != nil {
		return &sess, nil
	}

	// processing login response
	success := false
	for {
		tokchan := make(chan tokenStruct, 5)
		go processResponse(context.Background(), &sess, tokchan, nil)
		for tok := range tokchan {
			switch token := tok.(type) {
			case sspiMsg:
				sspi_msg, err := auth.NextBytes(token)
				if err != nil {
					return nil, err
				}
				if sspi_msg != nil && len(sspi_msg) > 0 {
					outbuf.BeginPacket(PackSSPIMessage, false)
					_, err = outbuf.Write(sspi_msg)
					if err != nil {
						return nil, err
					}
					err = outbuf.FinishPacket()
					if err != nil {
						return nil, err
					}
					sspi_msg = nil
				}
			case loginAckStruct:
				success = true
				sess.loginAck = token
			case error:
				return nil, errs.Wrap(token, "Login error")
			case doneStruct:
				if token.isError() {
					return nil, errs.Wrap(token.getError(), "Login error")
				}
				goto loginEnd
			}
		}
	}
loginEnd:
	if !success {
		return nil, fmt.Errorf("Login failed")
	}
	// TODO: how do we handle multiple jumps to initiate_connection
	//   the interceptor is currently designed for a single round
	if sess.routedServer != "" {
		toconn.Close()
		p.host = sess.routedServer
		p.port = uint64(sess.routedPort)
		if !p.hostInCertificateProvided {
			p.hostInCertificate = sess.routedServer
		}
		goto initiate_connection
	}
	return &sess, nil
}
