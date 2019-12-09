package mssql

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
)

func checkAzureSQLEnvironment(fedAuth string, t *testing.T) (*url.URL, string) {
	u := &url.URL{
		Scheme: "sqlserver",
		Host:   os.Getenv("SQL_SERVER"),
	}

	if u.Host == "" {
		t.Skip("Azure SQL Server name not provided in SQL_SERVER environment variable")
	}

	database := os.Getenv("SQL_DATABASE")
	if database == "" {
		t.Skip("Azure SQL database name not provided in SQL_DATABASE environment variable")
	}

	tenantID := os.Getenv("AZURE_TENANT_ID")
	if tenantID == "" {
		t.Skip("Azure tenant ID not provided in AZURE_TENANT_ID environment variable")
	}

	query := u.Query()

	query.Add("database", database)
	query.Add("encrypt", "true")
	query.Add("fedauth", fedAuth)

	u.RawQuery = query.Encode()

	return u, tenantID
}

func checkFedAuthUserPassword(t *testing.T) *url.URL {
	u, _ := checkAzureSQLEnvironment(fedAuthActiveDirectoryPassword, t)

	username := os.Getenv("SQL_AD_ADMIN_USER")
	password := os.Getenv("SQL_AD_ADMIN_PASSWORD")

	if username == "" || password == "" {
		t.Skip("Username and password login requires SQL_AD_ADMIN_USER and SQL_AD_ADMIN_PASSWORD environment variables")
	}

	u.User = url.UserPassword(username, password)

	return u
}

func checkFedAuthAppPassword(t *testing.T) *url.URL {
	u, tenantID := checkAzureSQLEnvironment(fedAuthActiveDirectoryApplication, t)

	appClientID := os.Getenv("APP_SP_CLIENT_ID")
	appPassword := os.Getenv("APP_SP_CLIENT_SECRET")

	if appClientID == "" || appPassword == "" {
		t.Skip("Application (service principal) login requires APP_SP_CLIENT_ID and APP_SP_CLIENT_SECRET environment variables")
	}

	u.User = url.UserPassword(appClientID+"@"+tenantID, appPassword)

	return u
}

func checkFedAuthAppCertPath(t *testing.T) *url.URL {
	u := checkFedAuthAppPassword(t)

	appCertPath := os.Getenv("APP_SP_CLIENT_CERT")
	if appCertPath == "" {
		t.Skip("Application (service principal) certificate login requires APP_SP_CLIENT_CERT with path to certificate")
	}

	query := u.Query()
	query.Add("clientcertpath", appCertPath)
	u.RawQuery = query.Encode()

	return u
}

func checkFedAuthVMSystemID(t *testing.T) (*url.URL, string) {
	u, tenantID := checkAzureSQLEnvironment(fedAuthActiveDirectoryMSI, t)

	vmClientID := os.Getenv("VM_CLIENT_ID")
	if vmClientID == "" {
		t.Skip("System-assigned identity login test requires VM_CLIENT_ID environment variable")
	}

	return u, vmClientID + "@" + tenantID
}

func checkFedAuthVMUserAssignedID(t *testing.T) (*url.URL, string) {
	u, tenantID := checkAzureSQLEnvironment(fedAuthActiveDirectoryMSI, t)

	uaClientID := os.Getenv("UA_CLIENT_ID")
	if uaClientID == "" {
		t.Skip("User-assigned identity login test requires UA_CLIENT_ID environment variable")
	}

	u.User = url.User(uaClientID)

	return u, uaClientID + "@" + tenantID
}

func checkLoggedInUser(expected string, u *url.URL, t *testing.T) {
	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		t.Fatalf("Failed to open URL %v: %v", u, err)
	}

	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sql := "SELECT SUSER_NAME()"

	stmt, err := db.PrepareContext(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to prepare query %s: %v", sql, err)
	}

	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("Failed to fetch query result for %s: %v", sql, err)
	}

	defer rows.Close()

	var username string
	if !rows.Next() {
		t.Fatalf("Empty result set for query %s", sql)
	}

	err = rows.Scan(&username)
	if err != nil {
		t.Fatalf("Failed to fetch first row for %s: %v", sql, err)
	}

	if !strings.EqualFold(username, expected) {
		t.Fatalf("Expected username %s: actual: %s", expected, username)
	}

	t.Logf("Logged in username %s matches expected %s", username, expected)
}

func TestFedAuthWithUserAndPassword(t *testing.T) {
	SetLogger(testLogger{t})
	u := checkFedAuthUserPassword(t)

	checkLoggedInUser(u.User.Username(), u, t)
}

func TestFedAuthWithApplicationUsingPassword(t *testing.T) {
	SetLogger(testLogger{t})
	u := checkFedAuthAppPassword(t)

	checkLoggedInUser(u.User.Username(), u, t)
}

func TestFedAuthWithApplicationUsingCertificate(t *testing.T) {
	SetLogger(testLogger{t})
	u := checkFedAuthAppCertPath(t)

	checkLoggedInUser(u.User.Username(), u, t)
}

func TestFedAuthWithSystemAssignedIdentity(t *testing.T) {
	u, vmName := checkFedAuthVMSystemID(t)
	SetLogger(testLogger{t})

	checkLoggedInUser(vmName, u, t)
}

func TestFedAuthWithUserAssignedIdentity(t *testing.T) {
	SetLogger(testLogger{t})
	u, uaName := checkFedAuthVMUserAssignedID(t)

	checkLoggedInUser(uaName, u, t)
}
