//go:build go1.18
// +build go1.18

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/azuread"
)

var (
	debug         = flag.Bool("debug", true, "enable debugging")
	password      = flag.String("password", "", "the client secret for the app/client ID")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "", "the database server")
	database      = flag.String("database", "", "the database name")
)

func main() {
	flag.Parse()

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" database:%s\n", *database)
	}

	connString := fmt.Sprintf("server=%s;password=%s;port=%d;database=%s;fedauth=ActiveDirectoryServicePrincipalAccessToken;", *server, *password, *port, *database)
	if *debug {
		fmt.Printf(" connString:%s\n", connString)
	}
	conn, err := sql.Open(azuread.DriverName, connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select 1, 'abc'")
	if err != nil {
		log.Fatal("Prepare failed:", err.Error())
	}
	defer stmt.Close()

	row := stmt.QueryRow()
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
