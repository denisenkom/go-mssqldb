package mssql

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"gopkg.in/jcmturner/gokrb5.v7/client"
	"gopkg.in/jcmturner/gokrb5.v7/config"
	"gopkg.in/jcmturner/gokrb5.v7/credentials"
	"gopkg.in/jcmturner/gokrb5.v7/keytab"
	"gopkg.in/jcmturner/gokrb5.v7/messages"
	"gopkg.in/jcmturner/gokrb5.v7/spnego"
	"gopkg.in/jcmturner/gokrb5.v7/types"
)

type Krb5ClientState int

const (
	ContextFlagREADY = 128
	/* initiator states */
	InitiatorStart Krb5ClientState = iota
	InitiatorRestart
	InitiatorWaitForMutal
	InitiatorReady
)

type krb5Auth struct {
	username          string
	realm             string
	service           string
	password          string
	port              string
	krb5ConfFile      string
	krbFile           string
	initkrbwithkeytab string
	krb5Client        *client.Client
	state             Krb5ClientState
}

var clientWithKeytab = client.NewClientWithKeytab
var loadCCache = credentials.LoadCCache
var clientFromCCache = client.NewClientFromCCache
var spnegoNewNegToken = spnego.NewNegTokenInitKRB5
var spnegoToken spnego.SPNEGOToken
var spnegoUnmarshal = spnegoToken.Unmarshal
var kt = &keytab.Keytab{}
var ktUnmarshal = kt.Unmarshal

var negTokenMarshal = func(negTok spnego.NegTokenInit) ([]byte, error) {
	return negTok.Marshal()
}
var getServiceTicket = func(cl *client.Client, spn string) (messages.Ticket, types.EncryptionKey, error) {
	return cl.GetServiceTicket(spn)
}

func getKRB5Auth(user, service, krb5Conf, krbFile, initkrbwithkeytab, password string) (auth, bool) {
	if krb5Conf == "" {
		krb5Conf = "/etc/krb5.conf"
	}
	var port string
	var realm string
	var serviceStr string

	params1 := strings.Split(service, ":")
	if len(params1) != 2 {
		return nil, false
	}

	params2 := strings.Split(params1[1], "@")
	if len(params2) == 1 {
		port = params1[1]
	} else if len(params2) == 2 {
		port = params2[0]
	} else if len(params2) != 2 {
		return nil, false
	}

	params3 := strings.Split(service, "@")
	if len(params3) == 1 {
		serviceStr = params3[0]
		params3 = strings.Split(params1[0], "/")
		params3 = strings.Split(params3[1], ".")
		realm = params3[1] + "." + params3[2]
	} else if len(params3) == 2 {
		realm = params3[1]
		serviceStr = params3[0]
	}

	return &krb5Auth{
		username:          user,
		service:           serviceStr,
		port:              port,
		realm:             realm,
		krb5ConfFile:      krb5Conf,
		krbFile:           krbFile,
		password:          password,
		initkrbwithkeytab: initkrbwithkeytab,
	}, true

}

func (auth *krb5Auth) InitialBytes() ([]byte, error) {

	krb5CnfFile, _ := os.Open(auth.krb5ConfFile)
	c, _ := config.NewConfigFromReader(krb5CnfFile)

	// Set to lookup KDCs in DNS
	c.LibDefaults.DNSLookupKDC = false

	var err error
	var cl *client.Client
	// Init keytab from conf
	if auth.initkrbwithkeytab == "true" {

		keytabConf, err := ioutil.ReadFile(auth.krbFile)
		if err != nil {
			return []byte{}, err
		}
		if err = ktUnmarshal([]byte(keytabConf)); err != nil {
			log.Printf("unmarshal keytabConf failed: %v", err)
			return []byte{}, err
		}

		// Init krb5 client and login
		cl = clientWithKeytab(auth.username, auth.realm, kt, c, client.DisablePAFXFAST(true))

	} else {
		cache, err := loadCCache(auth.krbFile)
		if err != nil {
			log.Println(err)
			return []byte{}, err
		}

		cl, err = clientFromCCache(cache, c)
		if err != nil {
			log.Println(err)
			return []byte{}, err
		}
	}

	auth.krb5Client = cl
	auth.state = InitiatorStart

	tkt, sessionKey, err := getServiceTicket(cl, auth.service)
	if err != nil {
		return []byte{}, err
	}

	negTok, err := spnegoNewNegToken(auth.krb5Client, tkt, sessionKey)
	if err != nil {
		fmt.Println(err)
		return []byte{}, err
	}

	outToken, err := negTokenMarshal(negTok)
	if err != nil {
		fmt.Println(err)
		return []byte{}, err
	}
	auth.state = InitiatorWaitForMutal
	return outToken, nil
}

func (auth *krb5Auth) Free() {
	auth.krb5Client.Destroy()
}

func (auth *krb5Auth) NextBytes(token []byte) ([]byte, error) {

	if err := spnegoUnmarshal(token); err != nil {
		err := fmt.Errorf("unmarshal APRep token failed: %w", err)
		return []byte{}, err
	}

	auth.state = InitiatorReady
	return []byte{}, nil
}
