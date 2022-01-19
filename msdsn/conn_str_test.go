package msdsn

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
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
		"disableretry=invalid",

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
		_, _, err := Parse(connStr)
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
		check   func(Config) bool
	}
	connStrings := []testStruct{
		{"server=server\\instance;database=testdb;user id=tester;password=pwd", func(p Config) bool {
			return p.Host == "server" && p.Instance == "instance" && p.User == "tester" && p.Password == "pwd"
		}},

		{"server=.", func(p Config) bool { return p.Host == "localhost" }},
		{"server=(local)", func(p Config) bool { return p.Host == "localhost" }},
		{"ServerSPN=serverspn;Workstation ID=workstid", func(p Config) bool { return p.ServerSPN == "serverspn" && p.Workstation == "workstid" }},
		{"failoverpartner=fopartner;failoverport=2000", func(p Config) bool { return p.FailOverPartner == "fopartner" && p.FailOverPort == 2000 }},
		{"app name=appname;applicationintent=ReadOnly;database=testdb", func(p Config) bool { return p.AppName == "appname" && p.ReadOnlyIntent }},
		{"encrypt=disable", func(p Config) bool { return p.Encryption == EncryptionDisabled }},
		{"encrypt=true", func(p Config) bool { return p.Encryption == EncryptionRequired }},
		{"encrypt=false", func(p Config) bool { return p.Encryption == EncryptionOff }},
		{"connection timeout=3;dial timeout=4;keepalive=5", func(p Config) bool {
			return p.ConnTimeout == 3*time.Second && p.DialTimeout == 4*time.Second && p.KeepAlive == 5*time.Second
		}},
		{"log=63", func(p Config) bool { return p.LogFlags == 63 && p.Port == 0 }},
		{"log=63;port=1000", func(p Config) bool { return p.LogFlags == 63 && p.Port == 1000 }},
		{"log=64", func(p Config) bool { return p.LogFlags == 64 }},
		{"log=64;packet size=0", func(p Config) bool { return p.LogFlags == 64 && p.PacketSize == 512 }},
		{"log=64;packet size=300", func(p Config) bool { return p.LogFlags == 64 && p.PacketSize == 512 }},
		{"log=64;packet size=8192", func(p Config) bool { return p.LogFlags == 64 && p.PacketSize == 8192 }},
		{"log=64;packet size=48000", func(p Config) bool { return p.LogFlags == 64 && p.PacketSize == 32767 }},
		{"disableretry=true", func(p Config) bool { return p.DisableRetry }},
		{"disableretry=false", func(p Config) bool { return !p.DisableRetry }},
		{"disableretry=1", func(p Config) bool { return p.DisableRetry }},
		{"disableretry=0", func(p Config) bool { return !p.DisableRetry }},
		{"", func(p Config) bool { return p.DisableRetry == disableRetryDefault }},

		// those are supported currently, but maybe should not be
		{"someparam", func(p Config) bool { return true }},
		{";;=;", func(p Config) bool { return true }},

		// ODBC mode
		{"odbc:server=somehost;user id=someuser;password=somepass", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "somepass"
		}},
		{"odbc:server=somehost;user id=someuser;password=some{pass", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some{pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={somepass}", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "somepass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some=pass}", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some=pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some;pass}", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some;pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some{pass}", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some{pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some}}pass}", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some}pass"
		}},
		{"odbc:server={somehost};user id={someuser};password={some{}}p=a;ss}", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some{}p=a;ss"
		}},
		{"odbc: server = somehost; user id =  someuser ; password = {some pass } ;", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "some pass "
		}},
		{"odbc:password", func(p Config) bool {
			return p.Password == ""
		}},
		{"odbc:", func(p Config) bool {
			return true
		}},
		{"odbc:password=", func(p Config) bool {
			return p.Password == ""
		}},
		{"odbc:password;", func(p Config) bool {
			return p.Password == ""
		}},
		{"odbc:password=;", func(p Config) bool {
			return p.Password == ""
		}},
		{"odbc:password={value}  ", func(p Config) bool {
			return p.Password == "value"
		}},
		{"odbc:server=somehost;user id=someuser;password=somepass;disableretry=true", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "somepass" && p.DisableRetry
		}},
		{"odbc:server=somehost;user id=someuser;password=somepass; disableretry =  1 ", func(p Config) bool {
			return p.Host == "somehost" && p.User == "someuser" && p.Password == "somepass" && p.DisableRetry
		}},

		// URL mode
		{"sqlserver://somehost?connection+timeout=30", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.ConnTimeout == 30*time.Second
		}},
		{"sqlserver://someuser@somehost?connection+timeout=30", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.User == "someuser" && p.Password == "" && p.ConnTimeout == 30*time.Second
		}},
		{"sqlserver://someuser:@somehost?connection+timeout=30", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.User == "someuser" && p.Password == "" && p.ConnTimeout == 30*time.Second
		}},
		{"sqlserver://someuser:foo%3A%2F%5C%21~%40;bar@somehost?connection+timeout=30", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.User == "someuser" && p.Password == "foo:/\\!~@;bar" && p.ConnTimeout == 30*time.Second
		}},
		{"sqlserver://someuser:foo%3A%2F%5C%21~%40;bar@somehost:1434?connection+timeout=30", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 1434 && p.Instance == "" && p.User == "someuser" && p.Password == "foo:/\\!~@;bar" && p.ConnTimeout == 30*time.Second
		}},
		{"sqlserver://someuser:foo%3A%2F%5C%21~%40;bar@somehost:1434/someinstance?connection+timeout=30", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 1434 && p.Instance == "someinstance" && p.User == "someuser" && p.Password == "foo:/\\!~@;bar" && p.ConnTimeout == 30*time.Second
		}},
		{"sqlserver://someuser@somehost?disableretry=true", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.User == "someuser" && p.Password == "" && p.DisableRetry
		}},
		{"sqlserver://someuser@somehost?connection+timeout=30&disableretry=1", func(p Config) bool {
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.User == "someuser" && p.Password == "" && p.ConnTimeout == 30*time.Second && p.DisableRetry
		}},
	}
	for _, ts := range connStrings {
		p, _, err := Parse(ts.connStr)
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

func TestConnParseRoundTripFixed(t *testing.T) {
	connStr := "sqlserver://sa:sa@localhost/sqlexpress?database=master&log=127"
	params, _, err := Parse(connStr)
	if err != nil {
		t.Fatal("Test URL is not valid", err)
	}
	rtParams, _, err := Parse(params.URL().String())
	if err != nil {
		t.Fatal("Params after roundtrip are not valid", err)
	}
	if !reflect.DeepEqual(params, rtParams) {
		t.Fatal("Parameters do not match after roundtrip", params, rtParams)
	}
}

func TestInvalidConnectionStringKerberos(t *testing.T) {
	connStrings := []string{
		"server=server;port=1345;realm=domain;trustservercertificate=true;keytabfile=/path/to/administrator2.keytab;enablekerberos=true",
		"server=server;port=1345;realm=domain;trustservercertificate=true;krbcache=;enablekerberos=true",
		"server=server;user id=user;password=pwd;port=1345;realm=domain;trustservercertificate=true;krb5conffile=/etc/krb5.conf;enablekerberos=true",
		"server=server;user id=user;password=pwd;port=1345;realm=domain;trustservercertificate=true;krb5conffile=/etc/krb5.conf;keytabfile=/path/to/administrator2.keytab;enablekerberos=true",
		"server=server;user id=user;port=1345;realm=domain;trustservercertificate=true;krb5conffile=/etc/krb5.conf;keytabfile=/path/to/administrator2.keytab;enablekerberos=true;initkrbwithkeytab=false",
		"server=server;user id=user;port=1345;realm=domain;trustservercertificate=true;krb5conffile=/etc/krb5.conf;enablekerberos=true;initkrbwithkeytab=true",
	}
	for _, connStr := range connStrings {
		_, _, err := Parse(connStr)
		if err == nil {
			t.Errorf("Connection expected to fail for connection string %s but it didn't", connStr)
			continue
		} else {
			t.Logf("Connection failed for %s as expected with error %v", connStr, err)
		}
	}
}

func TestValidConnectionStringKerberos(t *testing.T) {
	kerberosTestFile := createKrbFile("test.txt", t)
	connStrings := []string{
		"server=server;user id=user;port=1345;realm=domain;trustservercertificate=true;krb5conffile=" + kerberosTestFile + ";keytabfile=" + kerberosTestFile,
		"server=server;port=1345;realm=domain;trustservercertificate=true;krb5conffile=" + kerberosTestFile + ";krbcache=" + kerberosTestFile,
	}

	for _, connStr := range connStrings {
		_, _, err := Parse(connStr)
		if err == nil {
			t.Errorf("Connection string %s should fail to parse with error %s", connStrings, err)
		}
	}
	deleteFile(t)
}

func createKrbFile(filename string, t *testing.T) string {
	if _, err := os.Stat("temp"); os.IsNotExist(err) {
		err := os.Mkdir("temp", 0755)
		// TODO: handle error
		if err != nil {
			t.Errorf("Failed to create a temporary directory")
		}
	}
	file := []byte("This is a test file")
	err := ioutil.WriteFile("temp/"+filename, file, 0644)
	if err != nil {
		t.Errorf("Could not write file")
	}
	filedirectory := filepath.Dir(filename)
	thepath, _ := filepath.Abs(filedirectory)
	filePath := thepath + "/" + filename

	return filePath
}

func deleteFile(t *testing.T) {
	defer func() {
		err := os.RemoveAll("temp")
		if err != nil {
			t.Errorf("Could not delete directory")
		}
	}()
}
