package mssql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

type krb5Auth struct {
	username   string
	realm      string
	serverSPN  string
	password   string
	port       uint64
	krb5Config *config.Config
	krbKeytab  *keytab.Keytab
	krbCache   *credentials.CCache
	krb5Client *client.Client
	state      krb5ClientState
}

func getKRB5Auth(user, password, serverSPN string, krb5Conf *config.Config, keytabContent *keytab.Keytab, cacheContent *credentials.CCache) (auth, bool) {
	var port uint64
	var realm, serviceStr string
	var err error

	params1 := strings.Split(serverSPN, ":")
	if len(params1) != 2 {
		return nil, false
	}

	params2 := strings.Split(params1[1], "@")
	switch len(params2) {
	case 1:
		port, err = strconv.ParseUint(params1[1], 10, 16)
		if err != nil {
			return nil, false
		}
	case 2:
		port, err = strconv.ParseUint(params2[0], 10, 16)
		if err != nil {
			return nil, false
		}
	default:
		return nil, false
	}

	params3 := strings.Split(serverSPN, "@")
	switch len(params3) {
	case 1:
		serviceStr = params3[0]
		params3 = strings.Split(params1[0], "/")
		params3 = strings.Split(params3[1], ".")
		realm = params3[1] + "." + params3[2]
	case 2:
		realm = params3[1]
		serviceStr = params3[0]
	default:
		return nil, false
	}

	return &krb5Auth{
		username:   user,
		serverSPN:  serviceStr,
		port:       port,
		realm:      realm,
		krb5Config: krb5Conf,
		krbKeytab:  keytabContent,
		krbCache:   cacheContent,
		password:   password,
	}, true
}

func (auth *krb5Auth) InitialBytes() ([]byte, error) {
	var cl *client.Client
	var err error
	// Init keytab from conf
	if auth.krbKeytab != nil {
		// Init krb5 client and login
		cl = client.NewWithKeytab(auth.username, auth.realm, auth.krbKeytab, auth.krb5Config, client.DisablePAFXFAST(true))
	} else {
		cl, err = client.NewFromCCache(auth.krbCache, auth.krb5Config)
		if err != nil {
			return []byte{}, err
		}
	}
	auth.krb5Client = cl
	auth.state = initiatorStart
	tkt, sessionKey, err := cl.GetServiceTicket(auth.serverSPN)
	if err != nil {
		return []byte{}, err
	}

	negTok, err := spnego.NewNegTokenInitKRB5(auth.krb5Client, tkt, sessionKey)
	if err != nil {
		return []byte{}, err
	}

	outToken, err := negTok.Marshal()
	if err != nil {
		return []byte{}, err
	}
	auth.state = initiatorWaitForMutal
	return outToken, nil
}

func (auth *krb5Auth) Free() {
	auth.krb5Client.Destroy()
}

func (auth *krb5Auth) NextBytes(token []byte) ([]byte, error) {
	var spnegoToken spnego.SPNEGOToken
	if err := spnegoToken.Unmarshal(token); err != nil {
		err := fmt.Errorf("unmarshal APRep token failed: %w", err)
		return []byte{}, err
	}
	auth.state = initiatorReady
	return []byte{}, nil
}
