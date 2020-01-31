package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/Azure/go-autorest/autorest/adal"
	mssql "github.com/denisenkom/go-mssqldb"
)

var (
	debug    = flag.Bool("debug", false, "enable debugging")
	server   = flag.String("server", "", "the database server")
	database = flag.String("database", "", "the database")
)

func main() {
	flag.Parse()

	if *debug {
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" database:%s\n", *database)
	}

	if *server == "" {
		log.Fatal("Server name cannot be left empty")
	}

	if *database == "" {
		log.Fatal("Database name cannot be left empty")
	}

	connString := fmt.Sprintf("Server=%s;Database=%s", *server, *database)
	if *debug {
		fmt.Printf(" connString:%s\n", connString)
	}

	tokenProvider, err := getMSITokenProvider()
	if err != nil {
		log.Fatal("Error creating token provider for system assigned Azure Managed Identity:", err.Error())
	}

	connector, err := mssql.NewAccessTokenConnector(
		connString, tokenProvider)
	if err != nil {
		log.Fatal("Connector creation failed:", err.Error())
	}
	conn := sql.OpenDB(connector)
	defer conn.Close()

	row := conn.QueryRow("select 1, 'abc'")
	var somenumber int64
	var somechars string
	err = row.Scan(&somenumber, &somechars)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}
	fmt.Printf("somenumber:%d\n", somenumber)
	fmt.Printf("somechars:%s\n", somechars)

	fmt.Printf("bye\n")
}

func getMSITokenProvider() (func() (string, error), error) {
	msiEndpoint, err := adal.GetMSIEndpoint()
	if err != nil {
		return nil, err
	}
	msi, err := adal.NewServicePrincipalTokenFromMSI(
		msiEndpoint, "https://database.windows.net/")
	if err != nil {
		return nil, err
	}

	return func() (string, error) {
		msi.EnsureFresh()
		token := msi.OAuthToken()
		return token, nil
	}, nil
}
