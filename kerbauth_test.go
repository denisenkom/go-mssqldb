package mssql

import (
	"reflect"
	"testing"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
)

func TestGetKRB5Auth(t *testing.T) {
	krbConf := &config.Config{}
	krbKeytab := &keytab.Keytab{}
	krbCache := &credentials.CCache{}

	got, _ := getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com:1433", krbConf, krbKeytab, krbCache, true)
	keytab := &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5Config:        krbConf,
		krbKeytab:         krbKeytab,
		krbCache:          krbCache,
		initkrbwithkeytab: true,
		state:             0}

	res := reflect.DeepEqual(got, keytab)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", keytab, got)
	}

	got, _ = getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com:1433", krbConf, krbKeytab, krbCache, false)
	keytab = &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5Config:        krbConf,
		krbKeytab:         krbKeytab,
		krbCache:          krbCache,
		initkrbwithkeytab: false,
		state:             0}

	res = reflect.DeepEqual(got, keytab)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", keytab, got)
	}

	_, val := getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com", krbConf, krbKeytab, krbCache, true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object: no port defined")
	}

	got, _ = getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com:1433@DOMAIN.COM", krbConf, krbKeytab, krbCache, true)
	keytab = &krb5Auth{username: "",
		realm:             "DOMAIN.COM",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5Config:        krbConf,
		krbKeytab:         krbKeytab,
		krbCache:          krbCache,
		initkrbwithkeytab: true,
		state:             0}

	res = reflect.DeepEqual(got, keytab)
	if !res {
		t.Errorf("Failed to get correct krb5Auth object\nExpected:%v\nRecieved:%v", keytab, got)
	}

	_, val = getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com:1433@domain.com@test", krbConf, krbKeytab, krbCache, true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect serverSPN name")
	}

	_, val = getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com:port@domain.com", krbConf, krbKeytab, krbCache, true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect port")
	}

	_, val = getKRB5Auth("", "", "MSSQLSvc/mssql.domain.com:port", krbConf, krbKeytab, krbCache, true)
	if val {
		t.Errorf("Failed to get correct krb5Auth object due to incorrect port")
	}
}

func TestInitialBytes(t *testing.T) {
	krbConf := &config.Config{}
	krbKeytab := &keytab.Keytab{}
	krbCache := &credentials.CCache{}
	krbObj := &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5Config:        krbConf,
		krbKeytab:         krbKeytab,
		krbCache:          krbCache,
		initkrbwithkeytab: false,
		state:             0,
	}

	_, err := krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Failed to get Initial bytes")
	}

	krbObj.initkrbwithkeytab = true
	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf("Failed to get Initial bytes")
	}
}

func TestNextBytes(t *testing.T) {
	ans := []byte{}
	krbConf := &config.Config{}
	krbKeytab := &keytab.Keytab{}
	krbCache := &credentials.CCache{}

	var krbObj auth = &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5Config:        krbConf,
		krbKeytab:         krbKeytab,
		krbCache:          krbCache,
		initkrbwithkeytab: true,
		state:             0}

	_, err := krbObj.NextBytes(ans)
	if err == nil {
		t.Errorf("Error getting next byte")
	}
}

func TestFree(t *testing.T) {
	krbConf := &config.Config{}
	krbKeytab := &keytab.Keytab{}
	krbCache := &credentials.CCache{}
	kt := &keytab.Keytab{}
	c := &config.Config{}
	cl := client.NewWithKeytab("Administrator", "DOMAIN.COM", kt, c, client.DisablePAFXFAST(true))

	var krbObj auth = &krb5Auth{username: "",
		realm:             "domain.com",
		serverSPN:         "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5Config:        krbConf,
		krbKeytab:         krbKeytab,
		krbCache:          krbCache,
		initkrbwithkeytab: true,
		state:             0,
		krb5Client:        cl,
	}
	krbObj.Free()
}
