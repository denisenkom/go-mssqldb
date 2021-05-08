package azuread

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
)

const (
	fedAuthActiveDirectoryPassword    = "ActiveDirectoryPassword"
	fedAuthActiveDirectoryIntegrated  = "ActiveDirectoryIntegrated"
	fedAuthActiveDirectoryMSI         = "ActiveDirectoryMSI"
	fedAuthActiveDirectoryApplication = "ActiveDirectoryApplication"
)

// Federated authentication library affects the login data structure and message sequence.
const (
	// fedAuthLibraryLiveIDCompactToken specifies the Microsoft Live ID Compact Token authentication scheme
	fedAuthLibraryLiveIDCompactToken = 0x00

	// fedAuthLibrarySecurityToken specifies a token-based authentication where the token is available
	// without additional information provided during the login sequence.
	fedAuthLibrarySecurityToken = 0x01

	// fedAuthLibraryADAL specifies a token-based authentication where a token is obtained during the
	// login sequence using the server SPN and STS URL provided by the server during login.
	fedAuthLibraryADAL = 0x02

	// fedAuthLibraryReserved is used to indicate that no federated authentication scheme applies.
	fedAuthLibraryReserved = 0x7F
)

// Federated authentication ADAL workflow affects the mechanism used to authenticate.
const (
	// fedAuthADALWorkflowPassword uses a username/password to obtain a token from Active Directory
	fedAuthADALWorkflowPassword = 0x01

	// fedAuthADALWorkflowPassword uses the Windows identity to obtain a token from Active Directory
	fedAuthADALWorkflowIntegrated = 0x02

	// fedAuthADALWorkflowMSI uses the managed identity service to obtain a token
	fedAuthADALWorkflowMSI = 0x03
)

type azureFedAuthConfig struct {
	// The detected federated authentication library
	fedAuthLibrary int

	// Service principal logins
	clientID     string
	tenantID     string
	clientSecret string
	certificate  *x509.Certificate
	privateKey   *rsa.PrivateKey

	// ADAL workflows
	adalWorkflow byte
	user         string
	password     string
}

func validateParameters(params map[string]string) (p *azureFedAuthConfig, err error) {
	p = &azureFedAuthConfig{
		fedAuthLibrary: fedAuthLibraryReserved,
	}

	fedAuthWorkflow, _ := params["fedauth"]
	if fedAuthWorkflow == "" {
		return p, nil
	}

	switch {
	case strings.EqualFold(fedAuthWorkflow, fedAuthActiveDirectoryPassword):
		p.fedAuthLibrary = fedAuthLibraryADAL
		p.adalWorkflow = fedAuthADALWorkflowPassword
		p.user, _ = params["user id"]
		p.password, _ = params["password"]

	case strings.EqualFold(fedAuthWorkflow, fedAuthActiveDirectoryIntegrated):
		// Active Directory Integrated authentication is not fully supported:
		// you can only use this by also implementing an a token provider
		// and supplying it via ActiveDirectoryTokenProvider in the Connection.
		p.fedAuthLibrary = fedAuthLibraryADAL
		p.adalWorkflow = fedAuthADALWorkflowIntegrated

	case strings.EqualFold(fedAuthWorkflow, fedAuthActiveDirectoryMSI):
		// When using MSI, to request a specific client ID or user-assigned identity,
		// provide the ID in the "ad client id" parameter
		p.fedAuthLibrary = fedAuthLibraryADAL
		p.adalWorkflow = fedAuthADALWorkflowMSI
		p.clientID, _ = splitTenantAndClientID(params["user id"])

	case strings.EqualFold(fedAuthWorkflow, fedAuthActiveDirectoryApplication):
		p.fedAuthLibrary = fedAuthLibrarySecurityToken

		// Split the clientID@tenantID format
		p.clientID, p.tenantID = splitTenantAndClientID(params["user id"])
		if p.clientID == "" || p.tenantID == "" {
			return nil, errors.New("Must provide 'client id@tenant id' as username parameter when using ActiveDirectoryApplication authentication")
		}

		p.clientSecret, _ = params["password"]

		pemPath, _ := params["clientcertpath"]

		if pemPath == "" && p.clientSecret == "" {
			return nil, errors.New("Must provide 'password' parameter when using ActiveDirectoryApplication authentication without cert/key credentials")
		}

		if pemPath != "" {
			if p.certificate, p.privateKey, err = getFedAuthClientCertificate(pemPath, p.clientSecret); err != nil {
				return nil, err
			}

			p.clientSecret = ""
		}

	default:
		return nil, fmt.Errorf("Invalid federated authentication type '%s': expected %s, %s, %s or %s",
			fedAuthWorkflow, fedAuthActiveDirectoryPassword, fedAuthActiveDirectoryMSI,
			fedAuthActiveDirectoryApplication, fedAuthActiveDirectoryIntegrated)
	}

	return p, nil
}

func splitTenantAndClientID(user string) (string, string) {
	// Split the user name into client id and tenant id at the @ symbol
	at := strings.IndexRune(user, '@')
	if at < 1 || at >= (len(user)-1) {
		return user, ""
	}

	return user[0:at], user[at+1:]
}

func (p *azureFedAuthConfig) provideSecurityToken(ctx context.Context) (string, error) {
	switch {
	case p.certificate != nil && p.privateKey != nil:
		return SecurityTokenFromCertificate(ctx, p.clientID, p.tenantID, p.certificate, p.privateKey)
	case p.clientSecret != "":
		return SecurityTokenFromSecret(ctx, p.clientID, p.tenantID, p.clientSecret)
	}

	return "", errors.New("Client certificate and key, or client secret, required for service principal login")
}

func (p *azureFedAuthConfig) provideActiveDirectoryToken(ctx context.Context, serverSPN, stsURL string) (string, error) {
	switch p.adalWorkflow {
	case fedAuthADALWorkflowPassword:
		return ActiveDirectoryTokenFromPassword(ctx, serverSPN, stsURL, p.user, p.password)
	case fedAuthADALWorkflowMSI:
		return ActiveDirectoryTokenFromIdentity(ctx, serverSPN, stsURL, p.clientID)
	}

	return "", fmt.Errorf("ADAL workflow id %d not supported", p.adalWorkflow)
}

func getFedAuthClientCertificate(clientCertPath, clientCertPassword string) (certificate *x509.Certificate, privateKey *rsa.PrivateKey, err error) {
	pemBytes, err := ioutil.ReadFile(clientCertPath)
	if err != nil {
	}

	var block, encryptedPrivateKey *pem.Block
	var certificateBytes, privateKeyBytes []byte

	for block, pemBytes = pem.Decode(pemBytes); block != nil; block, pemBytes = pem.Decode(pemBytes) {
		_, dekInfo := block.Headers["DEK-Info"]
		switch {
		case block.Type == "CERTIFICATE":
			certificateBytes = block.Bytes
		case block.Type == "RSA PRIVATE KEY" && dekInfo:
			encryptedPrivateKey = block
		case block.Type == "RSA PRIVATE KEY":
			privateKeyBytes = block.Bytes
		default:
			return nil, nil, fmt.Errorf("PEM file %s contains unsupported block type %s", clientCertPath, block.Type)
		}
	}

	if len(certificateBytes) == 0 {
		return nil, nil, fmt.Errorf("No certificate found in PEM file at path %s: %v", clientCertPath, err)
	}

	certificate, err = x509.ParseCertificate(certificateBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to parse certificate found in PEM file at path %s: %v", clientCertPath, err)
	}

	if encryptedPrivateKey != nil {
		privateKeyBytes, err = x509.DecryptPEMBlock(encryptedPrivateKey, []byte(clientCertPassword))
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to decrypt private key found in PEM file at path %s: %v", clientCertPath, err)
		}
	}

	if len(privateKeyBytes) == 0 {
		return nil, nil, fmt.Errorf("No private key found in PEM file at path %s: %v", clientCertPath, err)
	}

	privateKey, err = x509.ParsePKCS1PrivateKey(privateKeyBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to parse private key found in PEM file at path %s: %v", clientCertPath, err)
	}

	return
}
