package mssql

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/jcmturner/gokrb5.v7/client"
	"gopkg.in/jcmturner/gokrb5.v7/config"
	"gopkg.in/jcmturner/gokrb5.v7/keytab"
)

func createKrbFile(filename string, t *testing.T) string {
	file := []byte("This is a test file")
	err := ioutil.WriteFile(filename, file, 0644)
	if err != nil {
		t.Errorf("Could not write file")
	}
	filedirectory := filepath.Dir(filename)
	thepath, _ := filepath.Abs(filedirectory)
	filePath := thepath + "/" + filename

	return filePath
}

func deleteFile(filename string, t *testing.T) {
	defer func() {
		if _, err := os.Stat(filename); err == nil {
			err = os.Remove(filename)
			if err != nil {
				t.Errorf("Could not delete file: %v", filename)
			}
		}
	}()
}

func TestGetKRB5Auth(t *testing.T) {
	keytabFile := createKrbFile("admin.keytab", t)
	got, _ := getKRB5Auth("", "MSSQLSvc/mssql.domain.com:1433", "/etc/krb5.conf", keytabFile, "", true)
	keytab := &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      "/etc/krb5.conf",
		krbFile:           keytabFile,
		initkrbwithkeytab: true,
		state:             0}

	res := reflect.DeepEqual(got, keytab)

	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", keytab, got)
	}

	krbcacheFile := createKrbFile("krb5ccache_1000", t)
	got, _ = getKRB5Auth("", "MSSQLSvc/mssql.domain.com:1433", "/etc/krb5.conf", krbcacheFile, "", true)
	keytab = &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      "/etc/krb5.conf",
		krbFile:           krbcacheFile,
		initkrbwithkeytab: true,
		state:             0}

	res = reflect.DeepEqual(got, keytab)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", keytab, got)
	}

	_, val := getKRB5Auth("", "MSSQLSvc/mssql.domain.com", "/etc/krb5.conf", keytabFile, "", true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object: no port defined")
	}

	got, _ = getKRB5Auth("", "MSSQLSvc/mssql.domain.com:1433@DOMAIN.COM", "/etc/krb5.conf", keytabFile, "", true)
	keytab = &krb5Auth{username: "",
		realm:             "DOMAIN.COM",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      "/etc/krb5.conf",
		krbFile:           keytabFile,
		initkrbwithkeytab: true,
		state:             0}

	res = reflect.DeepEqual(got, keytab)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", keytab, got)
	}

	_, val = getKRB5Auth("", "MSSQLSvc/mssql.domain.com:1433@domain.com@test", "", keytabFile, "", true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect serverSPN name")
	}

	_, val = getKRB5Auth("", "MSSQLSvc/mssql.domain.com:port@domain.com", "", keytabFile, "", true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect port")
	}

	_, val = getKRB5Auth("", "MSSQLSvc/mssql.domain.com:port", "", keytabFile, "", true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect port")
	}

	deleteFile(krbcacheFile, t)
	deleteFile(keytabFile, t)
}

func TestInitialBytes(t *testing.T) {
	krb5ConfFile := createKrbFile("krb5.conf", t)
	krbcacheFile := createKrbFile("krbcache_1000", t)
	krbObj := &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      krb5ConfFile,
		krbFile:           krbcacheFile,
		initkrbwithkeytab: false,
		state:             0,
	}

	_, err := krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Failed to get Initial bytes")
	}

	keytabFile := createKrbFile("admin.keytab", t)
	krbObj.krbFile = keytabFile
	krbObj.initkrbwithkeytab = true

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Failed to get Initial bytes")
	}

	krbObj.krb5ConfFile = "test/krb5.conf"
	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Should failed to get Initial bytes as the krb5.conf file path is wrong")
	}

	krbObj.krb5ConfFile = krb5ConfFile
	krbObj.krbFile = keytabFile + ".test"
	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Should failed to get Initial bytes as the krb5.conf file path is wrong")
	}

	deleteFile(krbcacheFile, t)
	deleteFile(keytabFile, t)
	deleteFile(krb5ConfFile, t)
}

func TestNextBytes(t *testing.T) {
	ans := []byte{}
	keytabFile := createKrbFile("admin.keytab", t)
	krb5ConfFile := createKrbFile("krb5.conf", t)
	var krbObj auth = &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      krb5ConfFile,
		krbFile:           keytabFile,
		initkrbwithkeytab: true,
		state:             0}

	_, err := krbObj.NextBytes(ans)
	if err == nil {
		t.Errorf("Error getting next byte")
	}

	deleteFile(keytabFile, t)
	deleteFile(krb5ConfFile, t)
}

func TestFree(t *testing.T) {
	keytabFile := createKrbFile("admin.keytab", t)
	krb5ConfFile := createKrbFile("krb5.conf", t)
	kt := &keytab.Keytab{}
	c := &config.Config{}
	cl := client.NewClientWithKeytab("Administrator", "DOMAIN.COM", kt, c, client.DisablePAFXFAST(true))
	var krbObj auth = &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      krb5ConfFile,
		krbFile:           keytabFile,
		initkrbwithkeytab: true,
		state:             0,
		krb5Client:        cl,
	}

	krbObj.Free()
	deleteFile(keytabFile, t)
	deleteFile(krb5ConfFile, t)
}
