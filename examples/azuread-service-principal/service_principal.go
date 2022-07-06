//go:build go1.18
// +build go1.18

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/microsoft/go-mssqldb/azuread"
)

var (
	debug         = flag.Bool("debug", true, "Enable debugging")
	password      = flag.String("password", "", "The client secret for the app/client ID")
	port     *int = flag.Int("port", 1433, "The database port")
	server        = flag.String("server", "", "The database server")
	user          = flag.String("user", "", "The app ID of the service principal. "+
		"Format: <app_id>@<tenant_id>. tenant_id is optional if the app and database are in the same tenant.")
	database = flag.String("database", "", "The database name")
)

func main() {
	flag.Parse()

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user:%s\n", *user)
		fmt.Printf(" database:%s\n", *database)
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;fedauth=ActiveDirectoryServicePrincipal;", *server, *user, *password, *port, *database)
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
