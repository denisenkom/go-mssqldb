package mssql

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/Azure/go-autorest/autorest/adal"
	"golang.org/x/crypto/pkcs12"
)

const (
	activeDirectoryEndpoint = "https://login.microsoftonline.com/"
	azureSQLResource        = "https://database.windows.net/"
	driverClientID          = "7f98cb04-cd1e-40df-9140-3bf7e2cea4db"
)

func fedAuthGetClientCertificate(clientCertPath, clientCertPassword string) (*x509.Certificate, *rsa.PrivateKey, error) {
	pkcs, err := ioutil.ReadFile(clientCertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to read the AD client certificate from path %s: %v", clientCertPath, err)
	}

	privateKey, certificate, err := pkcs12.Decode(pkcs, clientCertPassword)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to read the AD client certificate from path %s: %v", clientCertPath, err)
	}

	rsaPrivateKey, isRsaKey := privateKey.(*rsa.PrivateKey)
	if !isRsaKey {
		return nil, nil, fmt.Errorf("AD client certificate at path %s must contain an RSA private key", clientCertPath)
	}

	return certificate, rsaPrivateKey, nil
}

func fedAuthGetAccessToken(ctx context.Context, resource, tenantID string, p connectParams, log optionalLogger) (accessToken string, err error) {
	// The activeDirectoryEndpoint URL is used as a base against which the
	// tenant ID is resolved. When the workflow provides a complete endpoint
	// URL for the tenant, the URL resolution just returns that endpoint.
	oauthConfig, err := adal.NewOAuthConfig(activeDirectoryEndpoint, tenantID)
	if err != nil {
		log.Printf("Failed to obtain OAuth configuration for endpoint %s and tenant %s: %v", activeDirectoryEndpoint, tenantID, err)
		return "", err
	}

	var token *adal.ServicePrincipalToken
	if p.fedAuthLibrary == fedAuthLibrarySecurityToken {
		// When the security token library is used, the token is obtained without input
		// from the server, so the AD endpoint and Azure SQL resource URI are provided
		// from the constants above.
		if p.aadClientCertPath != "" {
			var certificate *x509.Certificate
			var rsaPrivateKey *rsa.PrivateKey
			certificate, rsaPrivateKey, err = fedAuthGetClientCertificate(p.aadClientCertPath, p.password)
			if err == nil {
				token, err = adal.NewServicePrincipalTokenFromCertificate(*oauthConfig, p.user, certificate, rsaPrivateKey, azureSQLResource)
			}
		} else {
			token, err = adal.NewServicePrincipalToken(*oauthConfig, p.user, p.password, azureSQLResource)
		}
	} else if p.fedAuthLibrary == fedAuthLibraryADAL {
		// When the ADAL workflow is used, the server provides the endpoint (STS URL)
		// and resource (server SPN) during the login process. The STS URL is passed
		// as the tenant ID and has already been used to build the OAuth config.
		if p.fedAuthADALWorkflow == fedAuthADALWorkflowPassword {
			token, err = adal.NewServicePrincipalTokenFromUsernamePassword(*oauthConfig, driverClientID, p.user, p.password, resource)

		} else if p.fedAuthADALWorkflow == fedAuthADALWorkflowMSI {
			// When using MSI, to request a specific client ID or user-assigned identity,
			// provide the ID as the username.
			var msiEndpoint string
			msiEndpoint, err = adal.GetMSIEndpoint()
			if err == nil {
				if p.user == "" {
					token, err = adal.NewServicePrincipalTokenFromMSI(msiEndpoint, resource)
				} else {
					token, err = adal.NewServicePrincipalTokenFromMSIWithUserAssignedID(msiEndpoint, resource, p.user)
				}
			}
		}
	} else {
		return "", errors.New("Unsupported federated authentication library")
	}

	if err != nil {
		log.Printf("Failed to obtain service principal token for client id %s in tenant %s: %v", p.user, tenantID, err)
		return "", err
	}

	err = token.RefreshWithContext(ctx)
	if err != nil {
		log.Printf("Failed to refresh service principal token for client id %s in tenant %s: %v", p.user, tenantID, err)
		return "", err
	}

	return token.Token().AccessToken, nil
}
