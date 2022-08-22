package msdsn

import (
	"crypto/tls"
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
		_, err := Parse(connStr)
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
		{"encrypt=disable;tlsmin=1.1", func(p Config) bool { return p.Encryption == EncryptionDisabled && p.TLSConfig == nil }},
		{"encrypt=true", func(p Config) bool { return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == 0 }},
		{"encrypt=true;tlsmin=1.0", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS10
		}},
		{"encrypt=false;tlsmin=1.0", func(p Config) bool {
			return p.Encryption == EncryptionOff && p.TLSConfig.MinVersion == tls.VersionTLS10
		}},
		{"encrypt=true;tlsmin=1.1", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS11
		}},
		{"encrypt=true;tlsmin=1.2", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS12
		}},
		{"encrypt=true;tlsmin=1.4", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == 0
		}},
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
		{"sqlserver://somehost?encrypt=true&tlsmin=1.1", func(p Config) bool {
			return p.Host == "somehost" && p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS11
		}},
	}
	for _, ts := range connStrings {
		p, err := Parse(ts.connStr)
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
	connStr := "sqlserver://sa:sa@localhost/sqlexpress?database=master&log=127&disableretry=true"
	params, err := Parse(connStr)
	if err != nil {
		t.Fatal("Test URL is not valid", err)
	}
	rtParams, err := Parse(params.URL().String())
	if err != nil {
		t.Fatal("Params after roundtrip are not valid", err)
	}
	if !reflect.DeepEqual(params, rtParams) {
		t.Fatal("Parameters do not match after roundtrip", params, rtParams)
	}
}

func TestAllKeysAreAvailableInParametersMap(t *testing.T) {
	keys := map[string]string{
		"user id":            "1",
		"testparam":          "testvalue",
		"password":           "test",
		"thisisanunknownkey": "thisisthevalue",
		"server":             "name",
	}

	connString := ""
	for key, val := range keys {
		connString += key + "=" + val + ";"
	}

	params, err := Parse(connString)
	if err != nil {
		t.Errorf("unexpected error while parsing, %v", err)
	}

	if params.Parameters == nil {
		t.Error("Expected parameters map to be instanciated, found nil")
		return
	}

	if len(params.Parameters) != len(keys) {
		t.Errorf("Expected parameters map to be same length as input map length, expected %v, found %v", len(keys), len(params.Parameters))
		return
	}

	for key, val := range keys {
		if params.Parameters[key] != val {
			t.Errorf("Expected parameters map to contain key %v and value %v, found %v", key, val, params.Parameters[key])
		}
	}
}
