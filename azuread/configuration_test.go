//go:build go1.18
// +build go1.18

package azuread

import (
	"reflect"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestValidateParameters(t *testing.T) {
	passphrase := "somesecret"
	certificatepath := "/user/cert/cert.pfx"
	appid := "applicationclientid=someguid"
	certprop := "clientcertpath=" + certificatepath
	tests := []struct {
		name     string
		dsn      string
		expected *azureFedAuthConfig
	}{
		{
			name:     "no fed auth configured",
			dsn:      "server=someserver",
			expected: &azureFedAuthConfig{fedAuthLibrary: mssql.FedAuthLibraryReserved},
		},
		{
			name: "application with cert/key",
			dsn:  `sqlserver://service-principal-id%40tenant-id:somesecret@someserver.database.windows.net?fedauth=ActiveDirectoryApplication&` + certprop + "&" + appid,
			expected: &azureFedAuthConfig{
				fedAuthLibrary:      mssql.FedAuthLibraryADAL,
				clientID:            "service-principal-id",
				tenantID:            "tenant-id",
				certificatePath:     certificatepath,
				clientSecret:        passphrase,
				adalWorkflow:        mssql.FedAuthADALWorkflowPassword,
				fedAuthWorkflow:     ActiveDirectoryApplication,
				applicationClientID: "someguid",
			},
		},
		{
			name: "application with cert/key missing tenant id",
			dsn:  "server=someserver.database.windows.net;fedauth=ActiveDirectoryApplication;user id=service-principal-id;password=somesecret;" + certprop + ";" + appid,
			expected: &azureFedAuthConfig{
				fedAuthLibrary:      mssql.FedAuthLibraryADAL,
				clientID:            "service-principal-id",
				certificatePath:     certificatepath,
				clientSecret:        passphrase,
				adalWorkflow:        mssql.FedAuthADALWorkflowPassword,
				fedAuthWorkflow:     ActiveDirectoryApplication,
				applicationClientID: "someguid",
			},
		},
		{
			name: "application with secret",
			dsn:  "server=someserver.database.windows.net;fedauth=ActiveDirectoryServicePrincipal;user id=service-principal-id@tenant-id;password=somesecret;",
			expected: &azureFedAuthConfig{
				clientID:        "service-principal-id",
				tenantID:        "tenant-id",
				clientSecret:    passphrase,
				adalWorkflow:    mssql.FedAuthADALWorkflowPassword,
				fedAuthWorkflow: ActiveDirectoryServicePrincipal,
			},
		},
		{
			name: "user with password",
			dsn:  "server=someserver.database.windows.net;fedauth=ActiveDirectoryPassword;user id=azure-ad-user@example.com;password=somesecret;" + appid,
			expected: &azureFedAuthConfig{
				adalWorkflow:        mssql.FedAuthADALWorkflowPassword,
				user:                "azure-ad-user@example.com",
				password:            passphrase,
				applicationClientID: "someguid",
				fedAuthWorkflow:     ActiveDirectoryPassword,
			},
		},
		{
			name: "managed identity without client id",
			dsn:  "server=someserver.database.windows.net;fedauth=ActiveDirectoryMSI",
			expected: &azureFedAuthConfig{
				adalWorkflow:    mssql.FedAuthADALWorkflowMSI,
				fedAuthWorkflow: ActiveDirectoryMSI,
			},
		},
		{
			name: "managed identity with client id",
			dsn:  "server=someserver.database.windows.net;fedauth=ActiveDirectoryManagedIdentity;user id=identity-client-id",
			expected: &azureFedAuthConfig{
				adalWorkflow:    mssql.FedAuthADALWorkflowMSI,
				clientID:        "identity-client-id",
				fedAuthWorkflow: ActiveDirectoryManagedIdentity,
			},
		},
		{
			name: "managed identity with resource id",
			dsn:  "server=someserver.database.windows.net;fedauth=ActiveDirectoryManagedIdentity;resource id=/subscriptions/{guid}/resourceGroups/{resource-group-name}/{resource-provider-namespace}/{resource-type}/{resource-name}",
			expected: &azureFedAuthConfig{
				adalWorkflow:    mssql.FedAuthADALWorkflowMSI,
				resourceID:      "/subscriptions/{guid}/resourceGroups/{resource-group-name}/{resource-provider-namespace}/{resource-type}/{resource-name}",
				fedAuthWorkflow: ActiveDirectoryManagedIdentity,
			},
		},
	}
	for _, tst := range tests {
		config, err := parse(tst.dsn)
		if tst.expected == nil {
			if err == nil {
				t.Errorf("No error returned when error expected in test case '%s'", tst.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("Error returned when none expected in test case '%s': %v", tst.name, err)
			continue
		}
		if tst.expected.fedAuthLibrary != mssql.FedAuthLibraryReserved {
			if tst.expected.fedAuthLibrary == 0 {
				tst.expected.fedAuthLibrary = mssql.FedAuthLibraryADAL
			}
		}
		// mssqlConfig is not idempotent due to pointers in it, plus we aren't testing its correctness here
		config.mssqlConfig = msdsn.Config{}
		if !reflect.DeepEqual(config, tst.expected) {
			t.Errorf("Captured parameters do not match in test case '%s'. Expected:%+v, Actual:%+v", tst.name, tst.expected, config)
		}
	}
}
