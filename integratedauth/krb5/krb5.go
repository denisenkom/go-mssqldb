//go:build !windows && go1.13
// +build !windows,go1.13

package krb5

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/microsoft/go-mssqldb/integratedauth"
	"github.com/microsoft/go-mssqldb/msdsn"
)

var (
	SetKrbConfig = setupKerbConfig
	SetKrbKeytab = setupKerbKeytab
	SetKrbCache  = setupKerbCache
)

// Kerberos Client State
type krb5ClientState int

type krb5Auth struct {
	username   string
	password   string
	realm      string
	serverSPN  string
	port       uint64
	krb5Config *config.Config
	krbKeytab  *keytab.Keytab
	krbCache   *credentials.CCache
	krb5Client *client.Client
	state      krb5ClientState
}

type Kerberos struct {
	// Kerberos configuration details
	Config *config.Config

	// Credential cache
	Cache *credentials.CCache

	// A Kerberos realm is the domain over which a Kerberos authentication server has the authority
	// to authenticate a user, host or service.
	Realm string

	// Kerberos keytab that stores long-term keys for one or more principals
	Keytab *keytab.Keytab
}

const (
	// Initiator states
	initiatorStart        krb5ClientState = iota
	initiatorWaitForMutal                 = iota + 2
	initiatorReady
)

var (
	_                integratedauth.IntegratedAuthenticator = (*krb5Auth)(nil)
	AuthProviderFunc integratedauth.Provider                = integratedauth.ProviderFunc(getAuth)
)

func init() {
	err := integratedauth.SetIntegratedAuthenticationProvider("krb5", AuthProviderFunc)
	if err != nil {
		panic(err)
	}
}

func getAuth(config msdsn.Config) (integratedauth.IntegratedAuthenticator, error) {
	var port uint64
	var realm, serviceStr string
	var err error

	krb, err := readKrb5Config(config)
	if err != nil {
		return &krb5Auth{}, err
	}
	params1 := strings.Split(config.ServerSPN, ":")
	if len(params1) != 2 {
		return nil, errors.New("invalid ServerSPN")
	}

	params2 := strings.Split(params1[1], "@")
	switch len(params2) {
	case 1:
		port, err = strconv.ParseUint(params1[1], 10, 16)
		if err != nil {
			return nil, err
		}
	case 2:
		port, err = strconv.ParseUint(params2[0], 10, 16)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("invalid ServerSPN")
	}

	params3 := strings.Split(config.ServerSPN, "@")
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
		return nil, errors.New("invalid ServerSPN")
	}

	return &krb5Auth{
		username:   config.User,
		password:   config.Password,
		serverSPN:  serviceStr,
		port:       port,
		realm:      realm,
		krb5Config: krb.Config,
		krbKeytab:  krb.Keytab,
		krbCache:   krb.Cache,
	}, nil
}

func (auth *krb5Auth) InitialBytes() ([]byte, error) {
	var cl *client.Client
	var err error
	// Init keytab from conf
	if auth.username != "" && auth.password != "" {
		cl = client.NewWithPassword(auth.username, auth.realm, auth.password, auth.krb5Config)
	} else if auth.krbKeytab != nil {
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

func readKrb5Config(config msdsn.Config) (Kerberos, error) {
	krb := Kerberos{}
	var err error

	krbConfig, ok := config.Parameters["krb5conffile"]
	if !ok {
		return krb, fmt.Errorf("krb5 config file is required")
	}

	krb.Config, err = SetKrbConfig(krbConfig)
	if err != nil {
		return krb, err
	}

	missingParam := validateKerbConfig(config.Parameters)
	if missingParam != "" {
		return krb, fmt.Errorf("missing parameter:%s", missingParam)
	}

	if realm, ok := config.Parameters["realm"]; ok {
		krb.Realm = realm
	}

	if krbCache, ok := config.Parameters["krbcache"]; ok {
		krb.Cache, err = SetKrbCache(krbCache)
		if err != nil {
			return krb, err
		}
	}

	if keytabfile, ok := config.Parameters["keytabfile"]; ok {
		krb.Keytab, err = SetKrbKeytab(keytabfile)
		if err != nil {
			return krb, err
		}
	}

	return krb, nil
}

func validateKerbConfig(c map[string]string) (missingParam string) {
	if c["keytabfile"] != "" {
		if c["realm"] == "" {
			missingParam = "realm"
			return
		}
	}
	if c["krbcache"] == "" && c["keytabfile"] == "" {
		missingParam = "atleast krbcache or keytab is required"
		return
	}
	return
}

func setupKerbConfig(krb5configPath string) (*config.Config, error) {
	krb5CnfFile, err := os.Open(krb5configPath)
	if err != nil {
		return nil, err
	}
	c, err := config.NewFromReader(krb5CnfFile)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func setupKerbCache(kerbCCahePath string) (*credentials.CCache, error) {
	cache, err := credentials.LoadCCache(kerbCCahePath)
	if err != nil {
		return nil, err
	}
	return cache, nil
}

func setupKerbKeytab(keytabFilePath string) (*keytab.Keytab, error) {
	var kt = &keytab.Keytab{}
	keytabConf, err := ioutil.ReadFile(keytabFilePath)
	if err != nil {
		return nil, err
	}
	if err = kt.Unmarshal([]byte(keytabConf)); err != nil {
		return nil, err
	}
	return kt, nil
}
