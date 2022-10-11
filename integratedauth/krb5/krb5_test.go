//go:build !windows && go1.13
// +build !windows,go1.13

package krb5

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/microsoft/go-mssqldb/integratedauth"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestGetAuth(t *testing.T) {
	kerberos := getKerberos()
	var err error
	configParams := msdsn.Config{
		User:      "",
		ServerSPN: "MSSQLSvc/mssql.domain.com:1433",
		Port:      1433,
		Parameters: map[string]string{
			"krb5conffile": "krb5conffile",
			"keytabfile":   "keytabfile",
			"krbcache":     "krbcache",
			"realm":        "domain.com",
		},
	}

	SetKrbConfig = func(krb5configPath string) (*config.Config, error) {
		return &config.Config{}, nil
	}
	SetKrbKeytab = func(keytabFilePath string) (*keytab.Keytab, error) {
		return &keytab.Keytab{}, nil
	}
	SetKrbCache = func(kerbCCahePath string) (*credentials.CCache, error) {
		return &credentials.CCache{}, nil
	}

	got, err := getAuth(configParams)
	if err != nil {
		t.Errorf("failed:%v", err)
	}
	kt := &krb5Auth{username: "",
		realm:      "domain.com",
		serverSPN:  "MSSQLSvc/mssql.domain.com:1433",
		port:       1433,
		krb5Config: kerberos.Config,
		krbKeytab:  kerberos.Keytab,
		krbCache:   kerberos.Cache,
		state:      0}

	res := reflect.DeepEqual(got, kt)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", kt, got)
	}

	configParams.ServerSPN = "MSSQLSvc/mssql.domain.com"

	_, val := getAuth(configParams)
	if val == nil {
		t.Errorf("Failed to get correct krb5Auth object: no port defined")
	}

	configParams.ServerSPN = "MSSQLSvc/mssql.domain.com:1433@DOMAIN.COM"

	got, _ = getAuth(configParams)
	kt = &krb5Auth{username: "",
		realm:      "DOMAIN.COM",
		serverSPN:  "MSSQLSvc/mssql.domain.com:1433",
		port:       1433,
		krb5Config: kerberos.Config,
		krbKeytab:  kerberos.Keytab,
		krbCache:   kerberos.Cache,
		state:      0}

	res = reflect.DeepEqual(got, kt)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", kt, got)
	}

	configParams.ServerSPN = "MSSQLSvc/mssql.domain.com:1433@domain.com@test"
	_, val = getAuth(configParams)
	if val == nil {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect serverSPN name")
	}

	configParams.ServerSPN = "MSSQLSvc/mssql.domain.com:port@domain.com"
	_, val = getAuth(configParams)
	if val == nil {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect port")
	}

	configParams.ServerSPN = "MSSQLSvc/mssql.domain.com:port"
	_, val = getAuth(configParams)
	if val == nil {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect port")
	}
}

func TestInitialBytes(t *testing.T) {
	kerberos := getKerberos()
	krbObj := &krb5Auth{username: "",
		realm:      "domain.com",
		serverSPN:  "MSSQLSvc/mssql.domain.com:1433",
		port:       1433,
		krb5Config: kerberos.Config,
		krbKeytab:  kerberos.Keytab,
		krbCache:   kerberos.Cache,
		state:      0,
	}

	_, err := krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Initial Bytes expected to fail but it didn't")
	}

	krbObj.krbKeytab = nil
	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Initial Bytes expected to fail but it didn't")
	}

}

func TestNextBytes(t *testing.T) {
	ans := []byte{}
	kerberos := getKerberos()

	var krbObj integratedauth.IntegratedAuthenticator = &krb5Auth{username: "",
		realm:      "domain.com",
		serverSPN:  "MSSQLSvc/mssql.domain.com:1433",
		port:       1433,
		krb5Config: kerberos.Config,
		krbKeytab:  kerberos.Keytab,
		krbCache:   kerberos.Cache,
		state:      0}

	_, err := krbObj.NextBytes(ans)
	if err == nil {
		t.Errorf("Next Byte expected to fail but it didn't")
	}
}

func TestFree(t *testing.T) {
	kerberos := getKerberos()
	kt := &keytab.Keytab{}
	c := &config.Config{}

	cl := client.NewWithKeytab("Administrator", "DOMAIN.COM", kt, c, client.DisablePAFXFAST(true))

	var krbObj integratedauth.IntegratedAuthenticator = &krb5Auth{username: "",
		realm:      "domain.com",
		serverSPN:  "MSSQLSvc/mssql.domain.com:1433",
		port:       1433,
		krb5Config: kerberos.Config,
		krbKeytab:  kerberos.Keytab,
		krbCache:   kerberos.Cache,
		state:      0,
		krb5Client: cl,
	}
	krbObj.Free()
	cacheEntries := len(kerberos.Cache.GetEntries())
	if cacheEntries != 0 {
		t.Errorf("Client not destroyed")
	}
}

func TestSetKrbConfig(t *testing.T) {
	krb5conffile := createTempFile(t, "krb5conffile")
	_, err := setupKerbConfig(krb5conffile)
	if err != nil {
		t.Errorf("Failed to read krb5 config file")
	}
}

func TestSetKrbKeytab(t *testing.T) {
	krbkeytab := createTempFile(t, "keytabfile")
	_, err := setupKerbKeytab(krbkeytab)
	if err == nil {
		t.Errorf("Failed to read keytab file")
	}
}

func TestSetKrbCache(t *testing.T) {
	krbcache := createTempFile(t, "krbcache")
	_, err := setupKerbCache(krbcache)
	if err == nil {
		t.Errorf("Failed to read cache file")
	}
}

func getKerberos() (krbParams *Kerberos) {
	krbParams = &Kerberos{
		Config: &config.Config{},
		Keytab: &keytab.Keytab{},
		Cache:  &credentials.CCache{},
	}
	return
}

func createTempFile(t *testing.T, filename string) string {
	file, err := ioutil.TempFile("", "test-"+filename+".txt")
	if err != nil {
		t.Fatalf("Failed to create a temp file:%v", err)
	}
	if _, err := file.Write([]byte("This is a test file\n")); err != nil {
		t.Fatalf("Failed to write file:%v", err)
	}
	return file.Name()
}
