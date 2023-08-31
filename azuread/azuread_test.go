//go:build go1.18
// +build go1.18

package azuread

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
)

func TestAzureSqlAuth(t *testing.T) {
	mssqlConfig := testConnParams(t, "")

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

func TestTDS8ConnWithAzureSqlAuth(t *testing.T) {
	mssqlConfig := testConnParams(t, ";encrypt=strict;TrustServerCertificate=false;tlsmin=1.2")
	conn, err := newConnectorConfig(mssqlConfig)
	if err != nil {
		t.Fatalf("Unable to get a connector: %v", err)
	}
	db := sql.OpenDB(conn)
	row := db.QueryRow("SELECT protocol_type, CONVERT(varbinary(9),protocol_version),client_net_address from sys.dm_exec_connections where session_id=@@SPID")
	if err != nil {
		t.Fatal("Prepare failed:", err.Error())
	}
	var protocolName string
	var tdsver []byte
	var clientAddress string
	err = row.Scan(&protocolName, &tdsver, &clientAddress)
	if err != nil {
		t.Fatal("Scan failed:", err.Error())
	}
	assert.Equal(t, "TSQL", protocolName, "Protocol name does not match")
	assert.Equal(t, "08000000", hex.EncodeToString(tdsver))
}

// returns parsed connection parameters derived from
// environment variables
func testConnParams(t testing.TB, dsnParams string) *azureFedAuthConfig {
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
	config, err := parse(dsn + dsnParams)
	if err != nil {
		t.Skip("error parsing connection string ")
	}
	if config.fedAuthLibrary == mssql.FedAuthLibraryReserved {
		t.Skip("Skipping azure test due to missing fedauth parameter ")
	}
	config.mssqlConfig.LogFlags = logFlags
	return config
}
