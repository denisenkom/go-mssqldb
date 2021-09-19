package mssql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/denisenkom/go-mssqldb/msdsn"
)

type MockTransport struct {
	bytes.Buffer
}

func (t *MockTransport) Close() error {
	return nil
}

func TestConstantsDefined(t *testing.T) {
	// This test is just here to avoid complaints about unused code.
	// These constants are part of the spec but not yet used.
	for _, b := range []byte{
		featExtSESSIONRECOVERY, featExtCOLUMNENCRYPTION, featExtGLOBALTRANSACTIONS,
		featExtAZURESQLSUPPORT, featExtDATACLASSIFICATION, featExtUTF8SUPPORT,
	} {
		if b == 0 {
			t.Fail()
		}
	}

	for _, i := range []int{
		fedAuthLibraryLiveIDCompactToken, fChangePassword, fSendYukonBinaryXML,
	} {
		if i < 0 {
			t.Fail()
		}
	}
}

func TestSendLogin(t *testing.T) {
	memBuf := new(MockTransport)
	buf := newTdsBuffer(1024, memBuf)
	login := login{
		TDSVersion:     verTDS73,
		PacketSize:     0x1000,
		ClientProgVer:  0x01060100,
		ClientPID:      100,
		ClientTimeZone: -4 * 60,
		ClientID:       [6]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab},
		OptionFlags1:   0xe0,
		OptionFlags3:   8,
		HostName:       "subdev1",
		UserName:       "test",
		Password:       "testpwd",
		AppName:        "appname",
		ServerName:     "servername",
		CtlIntName:     "library",
		Language:       "en",
		Database:       "database",
		ClientLCID:     0x204,
		AtchDBFile:     "filepath",
	}
	err := sendLogin(buf, &login)
	if err != nil {
		t.Error("sendLogin should succeed")
	}
	ref := []byte{
		16, 1, 0, 222, 0, 0, 1, 0, 198 + 16, 0, 0, 0, 3, 0, 10, 115, 0, 16, 0, 0, 0, 1,
		6, 1, 100, 0, 0, 0, 0, 0, 0, 0, 224, 0, 0, 8, 16, 255, 255, 255, 4, 2, 0,
		0, 94, 0, 7, 0, 108, 0, 4, 0, 116, 0, 7, 0, 130, 0, 7, 0, 144, 0, 10, 0, 0,
		0, 0, 0, 164, 0, 7, 0, 178, 0, 2, 0, 182, 0, 8, 0, 18, 52, 86, 120, 144, 171,
		198, 0, 0, 0, 198, 0, 8, 0, 214, 0, 0, 0, 0, 0, 0, 0, 115, 0, 117, 0, 98,
		0, 100, 0, 101, 0, 118, 0, 49, 0, 116, 0, 101, 0, 115, 0, 116, 0, 226, 165,
		243, 165, 146, 165, 226, 165, 162, 165, 210, 165, 227, 165, 97, 0, 112,
		0, 112, 0, 110, 0, 97, 0, 109, 0, 101, 0, 115, 0, 101, 0, 114, 0, 118, 0,
		101, 0, 114, 0, 110, 0, 97, 0, 109, 0, 101, 0, 108, 0, 105, 0, 98, 0, 114,
		0, 97, 0, 114, 0, 121, 0, 101, 0, 110, 0, 100, 0, 97, 0, 116, 0, 97, 0, 98,
		0, 97, 0, 115, 0, 101, 0, 102, 0, 105, 0, 108, 0, 101, 0, 112, 0, 97, 0,
		116, 0, 104, 0}
	out := memBuf.Bytes()
	if !bytes.Equal(ref, out) {
		t.Log("Expected:")
		t.Log(hex.Dump(ref))
		t.Log("Returned:")
		t.Log(hex.Dump(out))
		t.Fatal("input output don't match")
	}
}

func TestSendLoginWithFeatureExt(t *testing.T) {
	memBuf := new(MockTransport)
	buf := newTdsBuffer(1024, memBuf)
	login := login{
		TDSVersion:     verTDS74,
		PacketSize:     0x1000,
		ClientProgVer:  0x01060100,
		ClientPID:      100,
		ClientTimeZone: -4 * 60,
		ClientID:       [6]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab},
		OptionFlags1:   0xe0,
		OptionFlags3:   8,
		HostName:       "subdev1",
		AppName:        "appname",
		ServerName:     "servername",
		CtlIntName:     "library",
		Language:       "en",
		Database:       "database",
		ClientLCID:     0x204,
	}
	login.FeatureExt.Add(&featureExtFedAuth{
		FedAuthLibrary: fedAuthLibrarySecurityToken,
		FedAuthToken:   "fedauthtoken",
	})
	err := sendLogin(buf, &login)
	if err != nil {
		t.Error("sendLogin should succeed")
	}
	ref := []byte{
		16, 1, 0, 223, 0, 0, 1, 0, 215, 0, 0, 0, 4, 0, 0, 116,
		0, 16, 0, 0, 0, 1, 6, 1, 100, 0, 0, 0, 0, 0, 0, 0,
		224, 0, 0, 24, 16, 255, 255, 255, 4, 2, 0, 0, 94, 0, 7, 0,
		108, 0, 0, 0, 108, 0, 0, 0, 108, 0, 7, 0, 122, 0, 10, 0,
		176, 0, 4, 0, 142, 0, 7, 0, 156, 0, 2, 0, 160, 0, 8, 0,
		18, 52, 86, 120, 144, 171, 176, 0, 0, 0, 176, 0, 0, 0, 176, 0,
		0, 0, 0, 0, 0, 0, 115, 0, 117, 0, 98, 0, 100, 0, 101, 0,
		118, 0, 49, 0, 97, 0, 112, 0, 112, 0, 110, 0, 97, 0, 109, 0,
		101, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 110, 0,
		97, 0, 109, 0, 101, 0, 108, 0, 105, 0, 98, 0, 114, 0, 97, 0,
		114, 0, 121, 0, 101, 0, 110, 0, 100, 0, 97, 0, 116, 0, 97, 0,
		98, 0, 97, 0, 115, 0, 101, 0, 180, 0, 0, 0, 2, 29, 0, 0,
		0, 2, 24, 0, 0, 0, 102, 0, 101, 0, 100, 0, 97, 0, 117, 0,
		116, 0, 104, 0, 116, 0, 111, 0, 107, 0, 101, 0, 110, 0, 255}
	out := memBuf.Bytes()
	if !bytes.Equal(ref, out) {
		t.Log("Expected:")
		t.Log(hex.Dump(ref))
		t.Log("Returned:")
		t.Log(hex.Dump(out))
		t.Fatal("input output don't match")
	}
}

func TestSendSqlBatch(t *testing.T) {
	checkConnStr(t)
	p, _, err := msdsn.Parse(makeConnStr(t).String())
	if err != nil {
		t.Error("parseConnectParams failed:", err.Error())
		return
	}

	tl := testLogger{t: t}
	defer tl.StopLogging()
	conn, err := connect(context.Background(), &Connector{params: p}, optionalLogger{loggerAdapter{&tl}}, p)
	if err != nil {
		t.Error("Open connection failed:", err.Error())
		return
	}
	defer conn.buf.transport.Close()

	headers := []headerStruct{
		{hdrtype: dataStmHdrTransDescr,
			data: transDescrHdr{0, 1}.pack()},
	}
	err = sendSqlBatch72(conn.buf, "select 1", headers, true)
	if err != nil {
		t.Error("Sending sql batch failed", err.Error())
		return
	}

	reader := startReading(conn, context.Background(), outputs{})

	err = reader.iterateResponse()
	if err != nil {
		t.Fatal(err)
	}

	if len(reader.lastRow) == 0 {
		t.Fatal("expected row but no row set")
	}

	switch value := reader.lastRow[0].(type) {
	case int32:
		if value != 1 {
			t.Error("Invalid value returned, should be 1", value)
			return
		}
	}
}

// returns parsed connection parameters derived from
// environment variables
func testConnParams(t testing.TB) msdsn.Config {
	dsn := os.Getenv("SQLSERVER_DSN")
	const logFlags = 127
	if len(dsn) > 0 {
		params, _, err := msdsn.Parse(dsn)
		if err != nil {
			t.Fatal("unable to parse SQLSERVER_DSN", err)
		}
		params.LogFlags = logFlags
		return params
	}
	if len(os.Getenv("HOST")) > 0 && len(os.Getenv("DATABASE")) > 0 {
		return msdsn.Config{
			Host:     os.Getenv("HOST"),
			Instance: os.Getenv("INSTANCE"),
			Database: os.Getenv("DATABASE"),
			User:     os.Getenv("SQLUSER"),
			Password: os.Getenv("SQLPASSWORD"),
			LogFlags: logFlags,
		}
	}
	// try loading connection string from file
	f, err := os.Open(".connstr")
	if err == nil {
		rdr := bufio.NewReader(f)
		dsn, err := rdr.ReadString('\n')
		if err != io.EOF && err != nil {
			t.Fatal(err)
		}
		params, _, err := msdsn.Parse(dsn)
		if err != nil {
			t.Fatal("unable to parse connection string loaded from file", err)
		}
		params.LogFlags = logFlags
		return params
	}
	t.Skip("no database connection string")
	return msdsn.Config{}
}

func checkConnStr(t testing.TB) {
	testConnParams(t)
}

// makeConnStr returns a URL struct so it may be modified by various
// tests before used as a DSN.
func makeConnStr(t testing.TB) *url.URL {
	return testConnParams(t).URL()
}

type testLogger struct {
	t    testing.TB
	mu   sync.Mutex
	done bool
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.done {
		msg := fmt.Sprintf(format, v...)
		l.t.Logf("%v [%s]: %s", time.Now(), testLoggerCaller(), msg)
	}
}

func (l *testLogger) Println(v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.done {
		msg := fmt.Sprint(v...)
		l.t.Logf("%v [%s]: %s", time.Now(), testLoggerCaller(), msg)
	}
}

// testLoggerCaller returns a formatted string with the file and line number of the caller
// to the testLogger's Printf and Println functions. It accounts for Printf/Println being
// called directly or via the driver's optionalLogger.
func testLoggerCaller() string {
	caller := ""

	pc := make([]uintptr, 5)
	runtime.Callers(3, pc[:]) // skip Callers, testLogCaller, and testLogger.PrintX (always in the call stack)
	frames := runtime.CallersFrames(pc)

	// skip loggerAdapter.Log and optionalLogger.Log, if in the call stack
	for {
		frame, ok := frames.Next()
		if !ok {
			break
		}
		function := path.Base(frame.Function)
		if function != "go-mssqldb.loggerAdapter.Log" && function != "go-mssqldb.optionalLogger.Log" {
			caller = fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
			break
		}
	}

	return caller
}

func (l *testLogger) StopLogging() {
	l.mu.Lock()
	l.done = true
	l.mu.Unlock()
}

func testConnection(t *testing.T, connStr string) {
	conn, err := sql.Open("mssql", connStr)
	if err != nil {
		t.Fatal("Open connection failed:", err.Error())
		return
	}
	defer conn.Close()
	row := conn.QueryRow("select 1")
	var val int
	err = row.Scan(&val)
	if err != nil {
		t.Fatal("Scan failed:", err.Error())
	}
	if val != 1 {
		t.Fatalf("returned value %d does not match 1", val)
	}
}

func TestConnect(t *testing.T) {
	params := testConnParams(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)
	testConnection(t, params.URL().String())
}

func TestConnectViaIp(t *testing.T) {
	params := testConnParams(t)
	if params.Encryption == msdsn.EncryptionRequired {
		t.Skip("Unable to test connection to IP for servers that expect encryption")
	}

	if params.Host == "." {
		params.Host = "127.0.0.1"
	} else {
		ips, err := net.LookupIP(params.Host)
		if err != nil {
			t.Fatal("Unable to lookup IP", err)
		}
		params.Host = ips[0].String()
	}
	testConnection(t, params.URL().String())
}

func simpleQuery(conn *sql.DB, t *testing.T) (stmt *sql.Stmt) {
	stmt, err := conn.Prepare("select 1 as a")
	if err != nil {
		t.Error("Prepare failed:", err.Error())
		return nil
	}
	return stmt
}

func checkSimpleQuery(rows *sql.Rows, t *testing.T) {
	numrows := 0
	for rows.Next() {
		var val int
		err := rows.Scan(&val)
		if err != nil {
			t.Error("Scan failed:", err.Error())
		}
		if val != 1 {
			t.Error("query should return 1")
		}
		numrows++
	}
	if numrows != 1 {
		t.Error("query should return 1 row, returned", numrows)
	}
}

func TestQuery(t *testing.T) {
	conn, logger := open(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer logger.StopLogging()

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	stmt := simpleQuery(conn, t)
	if stmt == nil {
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Error("Query failed:", err.Error())
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Error("getting columns failed", err.Error())
	}
	if len(columns) != 1 && columns[0] != "a" {
		t.Error("returned incorrect columns (expected ['a']):", columns)
	}

	checkSimpleQuery(rows, t)
}

func TestMultipleQueriesSequentialy(t *testing.T) {

	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	stmt, err := conn.Prepare("select 1 as a")
	if err != nil {
		t.Error("Prepare failed:", err.Error())
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Error("Query failed:", err.Error())
		return
	}
	defer rows.Close()
	checkSimpleQuery(rows, t)

	rows, err = stmt.Query()
	if err != nil {
		t.Error("Query failed:", err.Error())
		return
	}
	defer rows.Close()
	checkSimpleQuery(rows, t)
}

func TestMultipleQueryClose(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	stmt, err := conn.Prepare("select 1 as a")
	if err != nil {
		t.Error("Prepare failed:", err.Error())
		return
	}
	defer stmt.Close()

	func() {
		rows, err := stmt.Query()
		if err != nil {
			t.Fatal("Query failed:", err.Error())
		}
		defer rows.Close()
	}()

	func() {
		rows, err := stmt.Query()
		if err != nil {
			t.Fatal("Query failed:", err.Error())
		}
		defer rows.Close()
		checkSimpleQuery(rows, t)
	}()
}

func TestPing(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	conn.Ping()
}

func TestSecureWithInvalidHostName(t *testing.T) {
	checkConnStr(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	dsn := makeConnStr(t)
	dsnParams := dsn.Query()
	dsnParams.Set("encrypt", "true")
	dsnParams.Set("TrustServerCertificate", "false")
	dsnParams.Set("hostNameInCertificate", "foo.bar")
	dsn.RawQuery = dsnParams.Encode()

	conn, err := sql.Open("mssql", dsn.String())
	if err != nil {
		t.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()
	err = conn.Ping()
	if err == nil {
		t.Fatal("Connected to fake foo.bar server")
	}
}

func TestSecureConnection(t *testing.T) {
	checkConnStr(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	dsn := makeConnStr(t)
	dsnParams := dsn.Query()
	dsnParams.Set("encrypt", "true")
	dsnParams.Set("TrustServerCertificate", "true")
	dsn.RawQuery = dsnParams.Encode()

	conn, err := sql.Open("mssql", dsn.String())
	if err != nil {
		t.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()
	var msg string
	err = conn.QueryRow("select 'secret'").Scan(&msg)
	if err != nil {
		t.Fatal("cannot scan value", err)
	}
	if msg != "secret" {
		t.Fatal("expected secret, got: ", msg)
	}
	var secure bool
	err = conn.QueryRow("select encrypt_option from sys.dm_exec_connections where session_id=@@SPID").Scan(&secure)
	if err != nil {
		t.Fatal("cannot scan value", err)
	}
	if !secure {
		t.Fatal("connection is not encrypted")
	}
}

func TestBadCredentials(t *testing.T) {
	params := testConnParams(t)
	params.Password = "padpwd"
	params.User = "baduser"
	testConnectionBad(t, params.URL().String())
}

func TestBadHost(t *testing.T) {
	params := testConnParams(t)
	params.Host = "badhost"
	params.Instance = ""
	testConnectionBad(t, params.URL().String())
}

func TestSSPIAuth(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Only on windows")
	}
	checkConnStr(t)
	connStr := makeConnStr(t)
	params := connStr.Query()
	params.Set("Integrated Security", "sspi")
	connStr.RawQuery = params.Encode()

	db, err := sql.Open("mssql", connStr.String())
	if err != nil {
		t.Error("Open failed", err)
	}
	defer db.Close()

	row := db.QueryRow("select 1, 'abc'")

	var somenumber int64
	var somechars string
	err = row.Scan(&somenumber, &somechars)
	if err != nil {
		t.Error("scan failed", err)
	}
	if somenumber != int64(1) || somechars != "abc" {
		t.Errorf("Invalid values from query: want {%d,'%s'}, got {%d,'%s'}", int64(1), "abc", somenumber, somechars)
	}
}

func TestUcs22Str(t *testing.T) {
	// Test valid input
	s, err := ucs22str([]byte{0x31, 0, 0x32, 0, 0x33, 0}) // 123 in UCS2 encoding
	if err != nil {
		t.Errorf("ucs22str should not fail for valid ucs2 byte sequence: %s", err)
	}
	if s != "123" {
		t.Errorf("ucs22str expected to return 123 but it returned %s", s)
	}

	// Test invalid input
	_, err = ucs22str([]byte{0})
	if err == nil {
		t.Error("ucs22str should fail on single byte input, but it didn't")
	}
}

func TestReadUcs2(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x31, 0, 0x32, 0, 0x33, 0}) // 123 in UCS2 encoding
	s, err := readUcs2(buf, 3)
	if err != nil {
		t.Errorf("readUcs2 should not fail for valid ucs2 byte sequence: %s", err)
	}
	if s != "123" {
		t.Errorf("readUcs2 expected to return 123 but it returned %s", s)
	}

	buf = bytes.NewBuffer([]byte{0})
	_, err = readUcs2(buf, 1)
	if err == nil {
		t.Error("readUcs2 should fail on single byte input, but it didn't")
	}
}

func TestReadUsVarChar(t *testing.T) {
	// should succeed for valid buffer
	buf := bytes.NewBuffer([]byte{3, 0, 0x31, 0, 0x32, 0, 0x33, 0}) // 123 in UCS2 encoding with length prefix 3 uint16
	s, err := readUsVarChar(buf)
	if err != nil {
		t.Errorf("readUsVarChar should not fail for valid ucs2 byte sequence: %s", err)
	}
	if s != "123" {
		t.Errorf("readUsVarChar expected to return 123 but it returned %s", s)
	}

	// should fail for empty buffer
	buf = bytes.NewBuffer([]byte{})
	_, err = readUsVarChar(buf)
	if err == nil {
		t.Error("readUsVarChar should fail on empty buffer, but it didn't")
	}
}

func TestReadBVarByte(t *testing.T) {
	memBuf := bytes.NewBuffer([]byte{3, 1, 2, 3})
	s, err := readBVarByte(memBuf)
	if err != nil {
		t.Errorf("readUsVarByte should not fail for valid buffer: %s", err)
	}
	if !bytes.Equal(s, []byte{1, 2, 3}) {
		t.Errorf("readUsVarByte expected to return [1 2 3] but it returned %v", s)
	}

	// test empty buffer
	memBuf = bytes.NewBuffer([]byte{})
	_, err = readBVarByte(memBuf)
	if err == nil {
		t.Error("readUsVarByte should fail on empty buffer, but it didn't")
	}

	// test short buffer
	memBuf = bytes.NewBuffer([]byte{1})
	_, err = readBVarByte(memBuf)
	if err == nil {
		t.Error("readUsVarByte should fail on short buffer, but it didn't")
	}
}

func BenchmarkPacketSize(b *testing.B) {
	checkConnStr(b)
	p, _, err := msdsn.Parse(makeConnStr(b).String())
	if err != nil {
		b.Error("parseConnectParams failed:", err.Error())
		return
	}

	benchmarks := []struct {
		name       string
		packetSize uint16
	}{
		{name: "PacketSize 2048", packetSize: 2048},
		{name: "PacketSize 4096", packetSize: 4096},
		{name: "PacketSize 8192", packetSize: 8192},
		{name: "PacketSize 16384", packetSize: 16384},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				p.PacketSize = bm.packetSize
				runBatch(b, p)
			}
		})
	}
}

func runBatch(t testing.TB, p msdsn.Config) {
	tl := testLogger{t: t}
	defer tl.StopLogging()
	conn, err := connect(context.Background(), &Connector{params: p}, optionalLogger{loggerAdapter{&tl}}, p)
	if err != nil {
		t.Error("Open connection failed:", err.Error())
		return
	}
	defer conn.buf.transport.Close()

	headers := []headerStruct{
		{hdrtype: dataStmHdrTransDescr,
			data: transDescrHdr{0, 1}.pack()},
	}
	err = sendSqlBatch72(conn.buf, "select 1", headers, true)
	if err != nil {
		t.Error("Sending sql batch failed", err.Error())
		return
	}

	reader := startReading(conn, context.Background(), outputs{})

	err = reader.iterateResponse()
	if err != nil {
		t.Fatal(err)
	}

	if len(reader.lastRow) == 0 {
		t.Fatal("expected row but no row set")
	}

	switch value := reader.lastRow[0].(type) {
	case int32:
		if value != 1 {
			t.Error("Invalid value returned, should be 1", value)
			return
		}
	}
}
