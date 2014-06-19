package mssql

import (
	"bytes"
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
	"time"
	"unicode/utf16"
	"unicode/utf8"
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

func getInstances(address string) (map[string]map[string]string, error) {
	conn, err := net.DialTimeout("udp", address+":1434", 5*time.Second)
	if err != nil {
		return nil, err
	}
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
const (
	packSQLBatch    = 1
	packRPCRequest  = 3
	packReply       = 4
	packCancel      = 6
	packBulkLoadBCP = 7
	packTransMgrReq = 14
	packNormal      = 15
	packLogin7      = 16
	packSSPIMessage = 17
	packPrelogin    = 18
)

// prelogin fields
// http://msdn.microsoft.com/en-us/library/dd357559.aspx
const (
	preloginVERSION    = 0
	preloginENCRYPTION = 1
	preloginINSTOPT    = 2
	preloginTHREADID   = 3
	preloginMARS       = 4
	preloginTRACEID    = 5
	preloginTERMINATOR = 0xff
)

const (
	encryptOff    = 0 // Encryption is available but off.
	encryptOn     = 1 // Encryption is available and on.
	encryptNotSup = 2 // Encryption is not available.
	encryptReq    = 3 // Encryption is required.
)

type tdsSession struct {
	buf      *tdsBuffer
	loginAck loginAckStruct
	database string
	columns  []columnStruct
	tranid   uint64
	logFlags uint64
}

const (
	logErrors      = 1
	logMessages    = 2
	logRows        = 4
	logSQL         = 8
	logParams      = 16
	logTransaction = 32
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
func writePrelogin(w *tdsBuffer, fields map[uint8][]byte) error {
	var err error

	w.BeginPacket(packPrelogin)
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
	err = w.WriteByte(preloginTERMINATOR)
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

func readPrelogin(r *tdsBuffer) (map[uint8][]byte, error) {
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
		if rec_type == preloginTERMINATOR {
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

// OptionFlags2
// http://msdn.microsoft.com/en-us/library/dd304019.aspx
const (
	fLanguageFatal = 1
	fODBC          = 2
	fTransBoundary = 4
	fCacheConnect  = 8
	fIntSecurity   = 0x80
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

func str2ucs2(s string) []byte {
	res := utf16.Encode([]rune(s))
	buf := new(bytes.Buffer)
	for _, item := range res {
		binary.Write(buf, binary.LittleEndian, item)
	}
	return buf.Bytes()
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

// http://msdn.microsoft.com/en-us/library/dd304019.aspx
func sendLogin(w *tdsBuffer, login login) error {
	w.BeginPacket(packLogin7)
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

func readUcs2(r io.Reader, numchars int) (res string, err error) {
	buf := make([]byte, numchars*2)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	return ucs22str(buf)
}

func readUsVarChar(r io.Reader) (res string, err error) {
	var numchars uint16
	err = binary.Read(r, binary.LittleEndian, &numchars)
	if err != nil {
		return "", err
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
	var numchars uint8
	err = binary.Read(r, binary.LittleEndian, &numchars)
	if err != nil {
		return "", err
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
	var length uint8
	err = binary.Read(r, binary.LittleEndian, &length)
	if err != nil {
		return
	}
	res = make([]byte, length)
	_, err = io.ReadFull(r, res)
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

func sendSqlBatch72(buf *tdsBuffer,
	sqltext string,
	headers []headerStruct) (err error) {
	buf.BeginPacket(packSQLBatch)

	writeAllHeaders(buf, headers)

	_, err = buf.Write(str2ucs2(sqltext))
	if err != nil {
		return err
	}
	return buf.FinishPacket()
}

type connectParams struct {
	logFlags               uint64
	port                   uint64
	host                   string
	instance               string
	database               string
	user                   string
	password               string
	dial_timeout           time.Duration
	conn_timeout           time.Duration
	keepAlive              time.Duration
	encrypt                bool
	disableEncryption      bool
	trustServerCertificate bool
	certificate            string
	hostInCertificate      string
}

func parseConnectParams(params map[string]string) (*connectParams, error) {
	var p connectParams
	strlog, ok := params["log"]
	if ok {
		var err error
		p.logFlags, err = strconv.ParseUint(strlog, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("Invalid log parameter '%s': %s", strlog, err.Error())
		}
	}
	server := params["server"]
	parts := strings.SplitN(server, "\\", 2)
	p.host = parts[0]
	if p.host == "." || strings.ToUpper(p.host) == "(LOCAL)" {
		p.host = "localhost"
	}
	if len(parts) > 1 {
		p.instance = parts[1]
	}
	p.database = params["database"]
	p.user = params["user id"]
	if len(p.user) == 0 {
		return nil, fmt.Errorf("Login failed, User Id is required")
	}
	p.password = params["password"]
	p.port = 1433
	if p.instance != "" {
		p.instance = strings.ToUpper(p.instance)
		instances, err := getInstances(p.host)
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
	} else {
		strport, ok := params["port"]
		if ok {
			var err error
			p.port, err = strconv.ParseUint(strport, 0, 16)
			if err != nil {
				f := "Invalid tcp port '%v': %v"
				return nil, fmt.Errorf(f, strport, err.Error())
			}
		}
	}
	p.dial_timeout = 5 * time.Second
	p.conn_timeout = 30 * time.Second
	strtimeout, ok := params["connection timeout"]
	if ok {
		timeout, err := strconv.ParseUint(strtimeout, 0, 16)
		if err != nil {
			f := "Invalid connection timeout '%v': %v"
			return nil, fmt.Errorf(f, strtimeout, err.Error())
		}
		p.dial_timeout = time.Duration(timeout) * time.Second
		p.conn_timeout = time.Duration(timeout) * time.Second
	}
	keepAlive, ok := params["keepAlive"]
	if ok {
		timeout, err := strconv.ParseUint(keepAlive, 0, 16)
		if err != nil {
			f := "Invalid keepAlive value '%s': %s"
			return nil, fmt.Errorf(f, keepAlive, err.Error())
		}
		p.keepAlive = time.Duration(timeout) * time.Second
	}
	encrypt, ok := params["encrypt"]
	if ok {
		if strings.ToUpper(encrypt) == "DISABLE" {
			p.disableEncryption = true
		} else {
			var err error
			p.encrypt, err = strconv.ParseBool(encrypt)
			if err != nil {
				f := "Invalid encrypt '%s': %s"
				return nil, fmt.Errorf(f, encrypt, err.Error())
			}
		}
	} else {
		p.trustServerCertificate = true
	}
	trust, ok := params["trustservercertificate"]
	if ok {
		var err error
		p.trustServerCertificate, err = strconv.ParseBool(trust)
		if err != nil {
			f := "Invalid trust server certificate '%s': %s"
			return nil, fmt.Errorf(f, trust, err.Error())
		}
	}
	p.certificate = params["certificate"]
	p.hostInCertificate, ok = params["hostnameincertificate"]
	if !ok {
		p.hostInCertificate = p.host
	}

	return &p, nil
}

func connect(params map[string]string) (res *tdsSession, err error) {
	p, err := parseConnectParams(params)
	if err != nil {
		return nil, err
	}
	addr := p.host + ":" + strconv.FormatUint(p.port, 10)
	d := net.Dialer{Timeout: p.dial_timeout, KeepAlive: p.keepAlive}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		f := "Unable to open tcp connection with host '%v': %v"
		return nil, fmt.Errorf(f, addr, err.Error())
	}

	toconn := NewTimeoutConn(conn, p.conn_timeout)

	outbuf := newTdsBuffer(4096, toconn)
	sess := tdsSession{
		buf:      outbuf,
		logFlags: p.logFlags,
	}

	instance_buf := []byte(p.instance)
	instance_buf = append(instance_buf, 0) // zero terminate instance name
	var encrypt byte
	if p.disableEncryption {
		encrypt = encryptNotSup
	} else if p.encrypt {
		encrypt = encryptOn
	} else {
		encrypt = encryptOff
	}
	fields := map[uint8][]byte{
		preloginVERSION:    {0, 0, 0, 0, 0, 0},
		preloginENCRYPTION: {encrypt},
		preloginINSTOPT:    instance_buf,
		preloginTHREADID:   {0, 0, 0, 0},
		preloginMARS:       {0}, // MARS disabled
	}

	err = writePrelogin(outbuf, fields)
	if err != nil {
		return nil, err
	}

	fields, err = readPrelogin(outbuf)
	if err != nil {
		return nil, err
	}

	encryptBytes, ok := fields[preloginENCRYPTION]
	if !ok {
		return nil, fmt.Errorf("Encrypt negotiation failed")
	}
	encrypt = encryptBytes[0]
	if p.encrypt && (encrypt == encryptNotSup || encrypt == encryptOff) {
		return nil, fmt.Errorf("Server does not support encryption")
	}

	if encrypt != encryptNotSup {
		var config tls.Config
		if p.certificate != "" {
			pem, err := ioutil.ReadFile(p.certificate)
			if err != nil {
				f := "Cannot read certificate '%s': %s"
				return nil, fmt.Errorf(f, p.certificate, err.Error())
			}
			certs := x509.NewCertPool()
			certs.AppendCertsFromPEM(pem)
			config.RootCAs = certs
		}
		if p.trustServerCertificate {
			config.InsecureSkipVerify = true
		}
		config.ServerName = p.hostInCertificate
		outbuf.transport = conn
		toconn.buf = outbuf
		tlsConn := tls.Client(toconn, &config)
		err = tlsConn.Handshake()
		toconn.buf = nil
		outbuf.transport = tlsConn
		if err != nil {
			f := "TLS Handshake failed: %s"
			return nil, fmt.Errorf(f, err.Error())
		}
		if encrypt == encryptOff {
			outbuf.afterFirst = func() {
				outbuf.transport = toconn
			}
		}
	}

	login := login{
		TDSVersion:   verTDS74,
		PacketSize:   uint32(len(outbuf.buf)),
		UserName:     p.user,
		Password:     p.password,
		Database:     p.database,
		OptionFlags2: fODBC, // to get unlimited TEXTSIZE
	}
	err = sendLogin(outbuf, login)
	if err != nil {
		return nil, err
	}

	// processing login response
	tokchan := make(chan tokenStruct, 5)
	go processResponse(&sess, tokchan)
	success := false
	for tok := range tokchan {
		switch token := tok.(type) {
		case loginAckStruct:
			success = true
			sess.loginAck = token
		case error:
			return nil, fmt.Errorf("Login error: %s", token.Error())
		}
	}
	if !success {
		return nil, fmt.Errorf("Login failed")
	}
	return &sess, nil
}
