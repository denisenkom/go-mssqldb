package msdsn

import (
	"crypto/tls"
	"encoding/hex"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		"multisubnetfailover=invalid",

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
		{"server=.", func(p Config) bool { return p.Host == "localhost" && !p.ColumnEncryption }},
		{"server=(local)", func(p Config) bool { return p.Host == "localhost" }},
		{"ServerSPN=serverspn;Workstation ID=workstid", func(p Config) bool { return p.ServerSPN == "serverspn" && p.Workstation == "workstid" }},
		{"failoverpartner=fopartner;failoverport=2000", func(p Config) bool { return p.FailOverPartner == "fopartner" && p.FailOverPort == 2000 }},
		{"app name=appname;applicationintent=ReadOnly;database=testdb", func(p Config) bool { return p.AppName == "appname" && p.ReadOnlyIntent }},
		{"encrypt=disable", func(p Config) bool { return p.Encryption == EncryptionDisabled }},
		{"encrypt=disable;tlsmin=1.1", func(p Config) bool { return p.Encryption == EncryptionDisabled && p.TLSConfig == nil }},
		{"encrypt=true", func(p Config) bool { return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == 0 }},
		{"encrypt=mandatory", func(p Config) bool { return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == 0 }},
		{"encrypt=true;tlsmin=1.0", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS10
		}},
		{"encrypt=false;tlsmin=1.0", func(p Config) bool {
			return p.Encryption == EncryptionOff && p.TLSConfig.MinVersion == tls.VersionTLS10
		}},
		{"encrypt=true;tlsmin=1.1;column encryption setting=enabled", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS11 && p.ColumnEncryption
		}},
		{"encrypt=true;tlsmin=1.2", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS12
		}},
		{"encrypt=true;tlsmin=1.3", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS13
		}},
		{"encrypt=true;tlsmin=1.4", func(p Config) bool {
			return p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == 0
		}},
		{"encrypt=false", func(p Config) bool { return p.Encryption == EncryptionOff }},
		{"encrypt=optional", func(p Config) bool { return p.Encryption == EncryptionOff }},
		{"encrypt=strict", func(p Config) bool { return p.Encryption == EncryptionStrict }},
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
		{"MultiSubnetFailover=true", func(p Config) bool { return p.MultiSubnetFailover }},
		{"MultiSubnetFailover=false", func(p Config) bool { return !p.MultiSubnetFailover }},

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
			return p.Host == "somehost" && p.Port == 0 && p.Instance == "" && p.User == "someuser" && p.Password == "" && p.ConnTimeout == 30*time.Second && p.DisableRetry && !p.ColumnEncryption
		}},
		{"sqlserver://somehost?encrypt=true&tlsmin=1.1&columnencryption=1", func(p Config) bool {
			return p.Host == "somehost" && p.Encryption == EncryptionRequired && p.TLSConfig.MinVersion == tls.VersionTLS11 && p.ColumnEncryption
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
	connStr := "sqlserver://sa:sa@localhost/sqlexpress?database=master&log=127&disableretry=true&dial+timeout=30"
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

func TestServerNameInTLSConfig(t *testing.T) {
	var tests = []struct {
		dsn          string
		host         string
		hasTLSConfig bool
	}{
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false&encrypt=true", "somehost", true},
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false&encrypt=false", "somehost", true},
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false&encrypt=true&hostnameincertificate=someotherhost", "someotherhost", true},
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false", "somehost", true},
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false&encrypt=DISABLE", "", false},
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false&encrypt=DISABLE&hostnameincertificate=someotherhost", "", false},
		{"sqlserver://someuser:somepass@somehost?TrustServerCertificate=false&encrypt=false", "somehost", true},
	}
	for _, test := range tests {
		cfg, err := Parse(test.dsn)
		if err != nil {
			t.Errorf("Could not parse valid connection string %s: %v", test.dsn, err)
		}
		if !test.hasTLSConfig && cfg.TLSConfig != nil {
			t.Errorf("Expected empty TLS config, but got %v (cfg.Host was %s)", cfg.TLSConfig, cfg.Host)
		}
		if test.hasTLSConfig && cfg.TLSConfig.ServerName != test.host {
			t.Errorf("Expected somehost as TLS server, but got %s (cfg.Host was %s)", cfg.TLSConfig.ServerName, cfg.Host)
		}
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

func TestReadCertificate(t *testing.T) {

	//Setup dummy certificate
	hexCertificate := "3082031830820200a00302010202103608db21691eccba415f8624d34b66fe300d06092a864886f70d01010b050030143112301006035504030c096c6f63616c686f7374301e170d3233303830383133343233375a170d3234303830383134303233375a30143112301006035504030c096c6f63616c686f737430820122300d06092a864886f70d01010105000382010f003082010a0282010100e18cd4d2923c548ac6e4fd731de116716a09fd2447feb28213810a1b508c22c108928f61531d31439b7252808d6bc6a71d50e5bb00596bbc1633d65389b80bb36f22d1546cbff570881331285cb458b3a2ad1ad0fa83081bd000f2793d29460a6adc0128a2d979d34f5cd91d60d4fef5932f393e04fcb3730a33693f3c44b882384c529f7489e58e296b0c17ca391b02f2488c38f8fc3c3afa0c1be0d22329287f93cf57ee46836a12f74de82eb54b18a5ae0134266db52633c0e33177f8ac4532045f053ddc920f0659cafa84c54c2b3cc92f4010c8af93ae0fc92e461d47c0cf2da46421189b2ddcf2f6ae17cb5ef6f1eda94452af6f714d583dcb7bcd43e90203010001a3663064300e0603551d0f0101ff0404030205a0301d0603551d250416301406082b0601050507030206082b0601050507030130140603551d11040d300b82096c6f63616c686f7374301d0603551d0e0416041443e3d9f187e9474d73794c641d54ecb810342ec6300d06092a864886f70d01010b05000382010100a227e721ac80838e66ef75d8ba080185dd8f4a5c84d7373e8ed50534100a490b577e3c1af593597303bdad8bb900e32b5d6f69941c19cc87fd426f9e4a4134f34f2ade02748d64031bc4e9c7617206a45c1d9556bb0488994cd27126adb029216f7c57852c1663983b7be638f1bc5411ba2221ce3fde29bf4818e36bec8ac25e9a37bfc41c5a3812829a6358a66c467818448346be140639957077b924b22567b75c7dab4d9d6794b4d79596d17446641684cbd193ec20a6faa85fb6b72f5f30dc57e8cd662b22152429e5b43ccb450c6840ba006e1c8e38b002aa97d8dd07e100ef76eebd9c523d8710636f060865e6198da620fedbf1ae6ed75df997641621"
	derfile, _ := os.CreateTemp("", "*.der")
	defer os.Remove(derfile.Name())
	certInBytes, _ := hex.DecodeString(hexCertificate)
	_, _ = derfile.Write(certInBytes)

	// Test with a valid certificate
	cert, err := readCertificate(derfile.Name())
	assert.Nil(t, err, "Expected no error while reading certificate, found %v", err)
	assert.NotNil(t, cert, "Expected certificate to be read, found nil")

	pemfile, _ := os.CreateTemp("", "*.pem")
	_, _ = io.Copy(derfile, pemfile)
	defer os.Remove(pemfile.Name())
	cert, err = readCertificate(pemfile.Name())
	assert.Nil(t, err, "Expected no error while reading certificate, found %v", err)
	assert.NotNil(t, cert, "Expected certificate to be read, found nil")

	// Test with an invalid certificate
	bakfile, _ := os.CreateTemp("", "*.bak")
	_, _ = io.Copy(derfile, bakfile)
	defer os.Remove(bakfile.Name())
	cert, err = readCertificate(bakfile.Name())
	assert.NotNil(t, err, "Expected error while reading certificate, found nil")
	assert.Nil(t, cert, "Expected certificate to be nil, found %v", cert)
}
