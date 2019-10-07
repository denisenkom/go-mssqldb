package mssql

import (
	"testing"
	"time"
)

func TestInvalidConnectionString(t *testing.T) {
	connStrings := []string{
		"log=invalid",
		"port=invalid",
		"packet size=invalid",
		"connection timeout=invalid",
		"dial timeout=invalid",
		"keepalive=invalid",
		"encrypt=invalid",
		"trustservercertificate=invalid",
		"failoverport=invalid",
		"applicationintent=ReadOnly",

		// ODBC mode
		"odbc:password={",
		"odbc:password={somepass",
		"odbc:password={somepass}}",
		"odbc:password={some}pass",
		"odbc:=", // unexpected =
		"odbc: =",
		"odbc:password={some} a",

		// URL mode
		"sqlserver://\x00",
		"sqlserver://host?key=value1&key=value2", // duplicate keys
	}
	for _, connStr := range connStrings {
		_, err := parseConnectParams(connStr)
		if err == nil {
			t.Errorf("Connection expected to fail for connection string %s but it didn't", connStr)
			continue
		} else {
			t.Logf("Connection failed for %s as expected with error %v", connStr, err)
		}
	}
}

func TestValidConnectionString(t *testing.T) {
	type testStruct struct {
		connStr string
		check   func(connectParams) bool
	}
	connStrings := []testStruct{
		{"server=server\\instance;database=testdb;user id=tester;password=pwd", func(p connectParams) bool {
			return p.host == "server" && p.instance == "instance" && p.user == "tester" && p.password == "pwd"
		}},
		{"server=.", func(p connectParams) bool { return p.host == "localhost" }},
		{"server=(local)", func(p connectParams) bool { return p.host == "localhost" }},
		{"ServerSPN=serverspn;Workstation ID=workstid", func(p connectParams) bool { return p.serverSPN == "serverspn" && p.workstation == "workstid" }},
		{"failoverpartner=fopartner;failoverport=2000", func(p connectParams) bool { return p.failOverPartner == "fopartner" && p.failOverPort == 2000 }},
		{"app name=appname;applicationintent=ReadOnly;database=testdb", func(p connectParams) bool { return p.appname == "appname" && (p.typeFlags&fReadOnlyIntent != 0) }},
		{"encrypt=disable", func(p connectParams) bool { return p.disableEncryption }},
		{"encrypt=true", func(p connectParams) bool { return p.encrypt && !p.disableEncryption }},
		{"encrypt=false", func(p connectParams) bool { return !p.encrypt && !p.disableEncryption }},
		{"trustservercertificate=true", func(p connectParams) bool { return p.trustServerCertificate }},
		{"trustservercertificate=false", func(p connectParams) bool { return !p.trustServerCertificate }},
		{"certificate=abc", func(p connectParams) bool { return p.certificate == "abc" }},
		{"hostnameincertificate=abc", func(p connectParams) bool { return p.hostInCertificate == "abc" }},
		{"connection timeout=3;dial timeout=4;keepalive=5", func(p connectParams) bool {
			return p.conn_timeout == 3*time.Second && p.dial_timeout == 4*time.Second && p.keepAlive == 5*time.Second
		}},
		{"log=63", func(p connectParams) bool { return p.logFlags == 63 && p.port == 1433 }},
		{"log=63;port=1000", func(p connectParams) bool { return p.logFlags == 63 && p.port == 1000 }},
		{"log=64", func(p connectParams) bool { return p.logFlags == 64 && p.packetSize == 4096 }},
		{"log=64;packet size=0", func(p connectParams) bool { return p.logFlags == 64 && p.packetSize == 512 }},
		{"log=64;packet size=300", func(p connectParams) bool { return p.logFlags == 64 && p.packetSize == 512 }},
		{"log=64;packet size=8192", func(p connectParams) bool { return p.logFlags == 64 && p.packetSize == 8192 }},
		{"log=64;packet size=48000", func(p connectParams) bool { return p.logFlags == 64 && p.packetSize == 32767 }},

		// those are supported currently, but maybe should not be
		{"someparam", func(p connectParams) bool { return true }},
		{";;=;", func(p connectParams) bool { return true }},

		// ODBC mode
		{"odbc:server=somehost;user id=someuser;password=somepass", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "somepass"
		}},
		{"odbc:server=somehost;user id=someuser;password=some{pass", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some{pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={somepass}", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "somepass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some=pass}", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some=pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some;pass}", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some;pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some{pass}", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some{pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some}}pass}", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some}pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some{}}p=a;ss}", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some{}p=a;ss"
		}},
		{"odbc: server = somehost; user id =  someuser ; password = {some pass } ;", func(p connectParams) bool {
			return p.host == "somehost" && p.user == "someuser" && p.password == "some pass "
		}},
		{"odbc:password", func(p connectParams) bool {
			return p.password == ""
		}},
		{"odbc:", func(p connectParams) bool {
			return true
		}},
		{"odbc:password=", func(p connectParams) bool {
			return p.password == ""
		}},
		{"odbc:password;", func(p connectParams) bool {
			return p.password == ""
		}},
		{"odbc:password=;", func(p connectParams) bool {
			return p.password == ""
		}},
		{"odbc:password={value}  ", func(p connectParams) bool {
			return p.password == "value"
		}},

		// URL mode
		{"sqlserver://somehost?connection+timeout=30", func(p connectParams) bool {
			return p.host == "somehost" && p.port == 1433 && p.instance == "" && p.conn_timeout == 30*time.Second
		}},
		{"sqlserver://someuser@somehost?connection+timeout=30", func(p connectParams) bool {
			return p.host == "somehost" && p.port == 1433 && p.instance == "" && p.user == "someuser" && p.password == "" && p.conn_timeout == 30*time.Second
		}},
		{"sqlserver://someuser:@somehost?connection+timeout=30", func(p connectParams) bool {
			return p.host == "somehost" && p.port == 1433 && p.instance == "" && p.user == "someuser" && p.password == "" && p.conn_timeout == 30*time.Second
		}},
		{"sqlserver://someuser:foo%3A%2F%5C%21~%40;bar@somehost?connection+timeout=30", func(p connectParams) bool {
			return p.host == "somehost" && p.port == 1433 && p.instance == "" && p.user == "someuser" && p.password == "foo:/\\!~@;bar" && p.conn_timeout == 30*time.Second
		}},
		{"sqlserver://someuser:foo%3A%2F%5C%21~%40;bar@somehost:1434?connection+timeout=30", func(p connectParams) bool {
			return p.host == "somehost" && p.port == 1434 && p.instance == "" && p.user == "someuser" && p.password == "foo:/\\!~@;bar" && p.conn_timeout == 30*time.Second
		}},
		{"sqlserver://someuser:foo%3A%2F%5C%21~%40;bar@somehost:1434/someinstance?connection+timeout=30", func(p connectParams) bool {
			return p.host == "somehost" && p.port == 1434 && p.instance == "someinstance" && p.user == "someuser" && p.password == "foo:/\\!~@;bar" && p.conn_timeout == 30*time.Second
		}},
	}
	for _, ts := range connStrings {
		p, err := parseConnectParams(ts.connStr)
		if err == nil {
			t.Logf("Connection string was parsed successfully %s", ts.connStr)
		} else {
			t.Errorf("Connection string %s failed to parse with error %s", ts.connStr, err)
			continue
		}

		if !ts.check(p) {
			t.Errorf("Check failed on conn str %s", ts.connStr)
		}
	}
}

func TestSplitConnectionStringURL(t *testing.T) {
	_, err := splitConnectionStringURL("http://bad")
	if err == nil {
		t.Error("Connection string with invalid scheme should fail to parse but it didn't")
	}
}
