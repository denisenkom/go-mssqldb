//go:build go1.18
// +build go1.18

package azuread

import (
	"bufio"
	"database/sql"
	"io"
	"os"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
)

func TestAzureSqlAuth(t *testing.T) {
	mssqlConfig := testConnParams(t)

	conn, err := newConnectorConfig(mssqlConfig)
	if err != nil {
		t.Fatalf("Unable to get a connector: %v", err)
	}
	db := sql.OpenDB(conn)
	row := db.QueryRow("select 100, suser_sname()")
	var val int
	var user string
	err = row.Scan(&val, &user)
	if err != nil {
		t.Fatalf("Unable to query the db: %v", err)
	}
	if val != 100 {
		t.Fatalf("Got wrong value from query. Expected:100, Got: %d", val)
	}
	t.Logf("Got suser_sname value %s", user)

}

// returns parsed connection parameters derived from
// environment variables
func testConnParams(t testing.TB) *azureFedAuthConfig {
	dsn := os.Getenv("AZURESERVER_DSN")
	const logFlags = 127
	if dsn == "" {
		// try loading connection string from file
		f, err := os.Open(".azureconnstr")
		if err == nil {
			rdr := bufio.NewReader(f)
			dsn, err = rdr.ReadString('\n')
			if err != io.EOF && err != nil {
				t.Fatal(err)
			}
		}
	}
	if dsn == "" {
		t.Skip("no azure database connection string. set AZURESERVER_DSN environment variable or create .azureconnstr file")
	}
	config, err := parse(dsn)
	if err != nil {
		t.Skip("error parsing connection string ")
	}
	if config.fedAuthLibrary == mssql.FedAuthLibraryReserved {
		t.Skip("Skipping azure test due to missing fedauth parameter ")
	}
	config.mssqlConfig.LogFlags = logFlags
	return config
}
