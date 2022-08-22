//go:build go1.18
// +build go1.18

package azuread

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

const (
	ActiveDirectoryDefault     = "ActiveDirectoryDefault"
	ActiveDirectoryIntegrated  = "ActiveDirectoryIntegrated"
	ActiveDirectoryPassword    = "ActiveDirectoryPassword"
	ActiveDirectoryInteractive = "ActiveDirectoryInteractive"
	// ActiveDirectoryMSI is a synonym for ActiveDirectoryManagedIdentity
	ActiveDirectoryMSI             = "ActiveDirectoryMSI"
	ActiveDirectoryManagedIdentity = "ActiveDirectoryManagedIdentity"
	// ActiveDirectoryApplication is a synonym for ActiveDirectoryServicePrincipal
	ActiveDirectoryApplication      = "ActiveDirectoryApplication"
	ActiveDirectoryServicePrincipal = "ActiveDirectoryServicePrincipal"
	scopeDefaultSuffix              = "/.default"
)

type azureFedAuthConfig struct {
	adalWorkflow byte
	mssqlConfig  msdsn.Config
	// The detected federated authentication library
	fedAuthLibrary  int
	fedAuthWorkflow string
	// Service principal logins
	clientID        string
	tenantID        string
	clientSecret    string
	certificatePath string
	resourceID      string

	// AD password/managed identity/interactive
	user                string
	password            string
	applicationClientID string
}

// parse returns a config based on an msdsn-style connection string
func parse(dsn string) (*azureFedAuthConfig, error) {
	mssqlConfig, err := msdsn.Parse(dsn)
	if err != nil {
		return nil, err
	}
	config := &azureFedAuthConfig{
		fedAuthLibrary: mssql.FedAuthLibraryReserved,
		mssqlConfig:    mssqlConfig,
	}

	err = config.validateParameters(mssqlConfig.Parameters)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (p *azureFedAuthConfig) validateParameters(params map[string]string) error {

	fedAuthWorkflow, _ := params["fedauth"]
	if fedAuthWorkflow == "" {
		return nil
	}

	p.fedAuthLibrary = mssql.FedAuthLibraryADAL

	p.applicationClientID, _ = params["applicationclientid"]

	switch {
	case strings.EqualFold(fedAuthWorkflow, ActiveDirectoryPassword):
		if p.applicationClientID == "" {
			return errors.New("applicationclientid parameter is required for " + ActiveDirectoryPassword)
		}
		p.adalWorkflow = mssql.FedAuthADALWorkflowPassword
		p.user, _ = params["user id"]
		p.password, _ = params["password"]
	case strings.EqualFold(fedAuthWorkflow, ActiveDirectoryIntegrated):
		// Active Directory Integrated authentication is not fully supported:
		// you can only use this by also implementing an a token provider
		// and supplying it via ActiveDirectoryTokenProvider in the Connection.
		p.adalWorkflow = mssql.FedAuthADALWorkflowIntegrated
	case strings.EqualFold(fedAuthWorkflow, ActiveDirectoryManagedIdentity) || strings.EqualFold(fedAuthWorkflow, ActiveDirectoryMSI):
		// When using MSI, to request a specific client ID or user-assigned identity,
		// provide the ID in the "user id" parameter
		p.adalWorkflow = mssql.FedAuthADALWorkflowMSI
		p.resourceID, _ = params["resource id"]
		p.clientID, _ = splitTenantAndClientID(params["user id"])
	case strings.EqualFold(fedAuthWorkflow, ActiveDirectoryApplication) || strings.EqualFold(fedAuthWorkflow, ActiveDirectoryServicePrincipal):
		p.adalWorkflow = mssql.FedAuthADALWorkflowPassword
		// Split the clientID@tenantID format
		// If no tenant is provided we'll use the one from the server
		p.clientID, p.tenantID = splitTenantAndClientID(params["user id"])
		if p.clientID == "" {
			return errors.New("Must provide 'client id[@tenant id]' as username parameter when using ActiveDirectoryApplication authentication")
		}

		p.clientSecret, _ = params["password"]

		p.certificatePath, _ = params["clientcertpath"]

		if p.certificatePath == "" && p.clientSecret == "" {
			return errors.New("Must provide 'password' parameter when using ActiveDirectoryApplication authentication without cert/key credentials")
		}
	case strings.EqualFold(fedAuthWorkflow, ActiveDirectoryDefault):
		p.adalWorkflow = mssql.FedAuthADALWorkflowPassword
	case strings.EqualFold(fedAuthWorkflow, ActiveDirectoryInteractive):
		if p.applicationClientID == "" {
			return errors.New("applicationclientid parameter is required for " + ActiveDirectoryInteractive)
		}
		p.adalWorkflow = mssql.FedAuthADALWorkflowPassword
		// user is an optional login hint
		p.user, _ = params["user id"]
		// we don't really have a password but we need to use some value.
		p.adalWorkflow = mssql.FedAuthADALWorkflowPassword

	default:
		return fmt.Errorf("Invalid federated authentication type '%s': expected one of %+v",
			fedAuthWorkflow,
			[]string{ActiveDirectoryApplication, ActiveDirectoryServicePrincipal, ActiveDirectoryDefault, ActiveDirectoryIntegrated, ActiveDirectoryInteractive, ActiveDirectoryManagedIdentity, ActiveDirectoryMSI, ActiveDirectoryPassword})
	}
	p.fedAuthWorkflow = fedAuthWorkflow
	return nil
}

func splitTenantAndClientID(user string) (string, string) {
	// Split the user name into client id and tenant id at the @ symbol
	at := strings.IndexRune(user, '@')
	if at < 1 || at >= (len(user)-1) {
		return user, ""
	}

	return user[0:at], user[at+1:]
}

func splitAuthorityAndTenant(authorityUrl string) (string, string) {
	separatorIndex := strings.LastIndex(authorityUrl, "/")
	tenant := authorityUrl[separatorIndex+1:]
	authority := authorityUrl[:separatorIndex]
	return authority, tenant
}

func (p *azureFedAuthConfig) provideActiveDirectoryToken(ctx context.Context, serverSPN, stsURL string) (string, error) {
	var cred azcore.TokenCredential
	var err error
	authority, tenant := splitAuthorityAndTenant(stsURL)
	// client secret connection strings may override the server tenant
	if p.tenantID != "" {
		tenant = p.tenantID
	}
	scope := stsURL
	if !strings.HasSuffix(serverSPN, scopeDefaultSuffix) {
		scope = strings.TrimRight(serverSPN, "/") + scopeDefaultSuffix
	}

	switch p.fedAuthWorkflow {
	case ActiveDirectoryServicePrincipal, ActiveDirectoryApplication:
		switch {
		case p.certificatePath != "":
			certData, err := os.ReadFile(p.certificatePath)
			if err != nil {
				certs, key, err := azidentity.ParseCertificates(certData, []byte(p.clientSecret))
				if err != nil {
					cred, err = azidentity.NewClientCertificateCredential(tenant, p.clientID, certs, key, nil)
				}
			}
		default:
			cred, err = azidentity.NewClientSecretCredential(tenant, p.clientID, p.clientSecret, nil)
		}
	case ActiveDirectoryPassword:
		cred, err = azidentity.NewUsernamePasswordCredential(tenant, p.applicationClientID, p.user, p.password, nil)
	case ActiveDirectoryMSI, ActiveDirectoryManagedIdentity:
		if p.resourceID != "" {
			cred, err = azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ResourceID(p.resourceID)})
		} else if p.clientID != "" {
			cred, err = azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(p.clientID)})
		} else {
			cred, err = azidentity.NewManagedIdentityCredential(nil)
		}
	case ActiveDirectoryInteractive:
		c := cloud.Configuration{ActiveDirectoryAuthorityHost: authority}
		config := azcore.ClientOptions{Cloud: c}
		cred, err = azidentity.NewInteractiveBrowserCredential(&azidentity.InteractiveBrowserCredentialOptions{ClientOptions: config, ClientID: p.applicationClientID})

	default:
		// Integrated just uses Default until azidentity adds Windows-specific authentication
		cred, err = azidentity.NewDefaultAzureCredential(nil)
	}

	if err != nil {
		return "", err
	}
	opts := policy.TokenRequestOptions{Scopes: []string{scope}}
	tk, err := cred.GetToken(ctx, opts)
	if err != nil {
		return "", err
	}
	return tk.Token, err
}
