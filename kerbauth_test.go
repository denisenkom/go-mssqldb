package mssql

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/jcmturner/gokrb5.v7/client"
	"gopkg.in/jcmturner/gokrb5.v7/config"
	"gopkg.in/jcmturner/gokrb5.v7/credentials"
	"gopkg.in/jcmturner/gokrb5.v7/keytab"
	"gopkg.in/jcmturner/gokrb5.v7/messages"
	"gopkg.in/jcmturner/gokrb5.v7/spnego"
	"gopkg.in/jcmturner/gokrb5.v7/types"
)

func createKrbFile(filename string, t *testing.T) string {
	//The byte array is used to create a basic file for testing purpose
	ans := []byte{84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 102, 105, 108, 101, 46}
	err := ioutil.WriteFile(filename, ans, 0644)
	if err != nil {
		t.Errorf("Could not write file")
	}
	filedirectory := filepath.Dir(filename)
	thepath, _ := filepath.Abs(filedirectory)
	filePath := thepath + "/" + filename

	return filePath
}

func deleteFile(filename string, t *testing.T) {
	if _, err := os.Stat(filename); err == nil {
		err = os.Remove(filename)
		if err != nil {
			t.Errorf("Could not delete file: %v", filename)
		}
	}
}

func TestGetKRB5Auth(t *testing.T) {
	keytabFile := createKrbFile("admin.keytab", t)
	got, _ := getKRB5Auth("", "MSSQLSvc/mssql.domain.com:1433", "/etc/krb5.conf", keytabFile, "", true)
	var keytab auth = &krb5Auth{username: "",
		realm:             "domain.com",
		service:           "MSSQLSvc/mssql.domain.com:1433",
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
		service:           "MSSQLSvc/mssql.domain.com:1433",
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
		t.Errorf("Failed to get correct krb5Auth object")
	}

	got, _ = getKRB5Auth("", "MSSQLSvc/mssql.domain.com:1433@DOMAIN.COM", "/etc/krb5.conf", keytabFile, "", true)
	keytab = &krb5Auth{username: "",
		realm:             "DOMAIN.COM",
		service:           "MSSQLSvc/mssql.domain.com:1433",
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
		t.Errorf("Failed to get correct krb5Auth object due to incorrect service name")
	}

	defer deleteFile(krbcacheFile, t)
	defer deleteFile(keytabFile, t)

}

func TestInitialBytes(t *testing.T) {

	krbcacheFile := createKrbFile("krbcache_1000", t)
	krbObj := &krb5Auth{username: "",
		realm:             "domain.com",
		service:           "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      "/etc/krb5.conf",
		krbFile:           krbcacheFile,
		initkrbwithkeytab: false,
		state:             0,
	}

	loadCCache = func(cpath string) (*credentials.CCache, error) {
		return &credentials.CCache{}, nil
	}

	clientFromCCache = func(c *credentials.CCache, krb5conf *config.Config, settings ...func(*client.Settings)) (*client.Client, error) {
		return &client.Client{}, nil
	}

	getServiceTicket = func(cl *client.Client, spn string) (messages.Ticket, types.EncryptionKey, error) {
		return messages.Ticket{}, types.EncryptionKey{}, nil
	}
	spnegoNewNegToken = func(cl *client.Client, tkt messages.Ticket, sessionKey types.EncryptionKey) (spnego.NegTokenInit, error) {
		return spnego.NegTokenInit{}, nil
	}

	_, err := krbObj.InitialBytes()
	if err != nil {
		t.Errorf(err.Error())
	}

	loadCCache = func(cpath string) (*credentials.CCache, error) {
		return &credentials.CCache{}, errors.New("Error loading cache file")
	}

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	loadCCache = func(cpath string) (*credentials.CCache, error) {
		return &credentials.CCache{}, nil
	}
	clientFromCCache = func(c *credentials.CCache, krb5conf *config.Config, settings ...func(*client.Settings)) (*client.Client, error) {
		return &client.Client{}, errors.New("Failed to create a client from CCache")
	}
	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	clientFromCCache = func(c *credentials.CCache, krb5conf *config.Config, settings ...func(*client.Settings)) (*client.Client, error) {
		return &client.Client{}, nil
	}
	getServiceTicket = func(cl *client.Client, spn string) (messages.Ticket, types.EncryptionKey, error) {
		return messages.Ticket{}, types.EncryptionKey{}, errors.New("Failed to create service ticket")
	}

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	keytabFile := createKrbFile("admin.keytab", t)
	krbObj.initkrbwithkeytab = true
	krbObj.krbFile = keytabFile

	getServiceTicket = func(cl *client.Client, spn string) (messages.Ticket, types.EncryptionKey, error) {
		return messages.Ticket{}, types.EncryptionKey{}, nil
	}
	ktUnmarshal = func(b []byte) error {
		return nil
	}

	clientWithKeytab = func(username string, realm string, kt *keytab.Keytab, krb5conf *config.Config, settings ...func(*client.Settings)) *client.Client {
		return &client.Client{}
	}

	_, err = krbObj.InitialBytes()
	if err != nil {
		t.Errorf(err.Error())
	}

	krbObj.krbFile = "Test"

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	krbObj.krbFile = keytabFile
	ktUnmarshal = func(b []byte) error {
		return errors.New("Failed to unmarshal keytab file")
	}

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	ktUnmarshal = func(b []byte) error {
		return nil
	}

	spnegoNewNegToken = func(cl *client.Client, tkt messages.Ticket, sessionKey types.EncryptionKey) (spnego.NegTokenInit, error) {
		return spnego.NegTokenInit{}, errors.New("Failed to create a new spnego token")
	}

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	spnegoNewNegToken = func(cl *client.Client, tkt messages.Ticket, sessionKey types.EncryptionKey) (spnego.NegTokenInit, error) {
		return spnego.NegTokenInit{}, nil
	}

	negTokenMarshal = func(negTok spnego.NegTokenInit) ([]byte, error) {
		return []byte{}, errors.New("Failed to marshal neg token")
	}

	_, err = krbObj.InitialBytes()
	if err == nil {
		t.Errorf(err.Error())
	}

	defer deleteFile(krbcacheFile, t)
	defer deleteFile(keytabFile, t)
}

func TestNextBytes(t *testing.T) {
	ans := []byte{84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 102, 105, 108, 101, 46}
	spnegoUnmarshal = func(b []byte) error {
		return nil
	}

	keytabFile := createKrbFile("admin.keytab", t)
	var krbObj auth = &krb5Auth{username: "",
		realm:             "domain.com",
		service:           "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      "/etc/krb5.conf",
		krbFile:           keytabFile,
		initkrbwithkeytab: true,
		state:             0}

	_, err := krbObj.NextBytes(ans)
	if err != nil {
		t.Errorf("Error getting next byte")
	}

	spnegoUnmarshal = func(b []byte) error {
		return errors.New("Failed to unmarshal")
	}

	_, err = krbObj.NextBytes(ans)
	if err == nil {
		t.Errorf("Should fail to unmarshal but passed")
	}

	defer deleteFile(keytabFile, t)
}

func TestFree(t *testing.T) {
	keytabFile := createKrbFile("admin.keytab", t)
	kt := &keytab.Keytab{}
	c := &config.Config{}
	cl := client.NewClientWithKeytab("Administrator", "DOMAIN.COM", kt, c, client.DisablePAFXFAST(true))
	var krbObj auth = &krb5Auth{username: "",
		realm:             "domain.com",
		service:           "MSSQLSvc/mssql.domain.com:1433",
		password:          "",
		port:              1433,
		krb5ConfFile:      "/etc/krb5.conf",
		krbFile:           keytabFile,
		initkrbwithkeytab: true,
		state:             0,
		krb5Client:        cl,
	}

	krbObj.Free()
	defer deleteFile(keytabFile, t)
}
