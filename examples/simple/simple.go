package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/denisenkom/go-mssqldb/azuread"
)

var (
	database      = flag.String("database", "", "the database name")
	debug         = flag.Bool("debug", false, "enable debugging")
	dsn           = flag.String("dsn", os.Getenv("SQLSERVER_DSN"), "complete SQL DSN")
	password      = flag.String("password", "", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "", "the database server")
	user          = flag.String("user", "", "the database user")
)

func main() {
	flag.Parse()

	var connString string

	if *dsn == "" {
		if *debug {
			fmt.Printf(" server:   %s\n", *server)
			fmt.Printf(" port:     %d\n", *port)
			fmt.Printf(" user:     %s\n", *user)
			fmt.Printf(" password: %s\n", *password)
			fmt.Printf(" database: %s\n", *database)
		}

		connString = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=true",
			url.QueryEscape(*user), url.QueryEscape(*password),
			url.QueryEscape(*server), *port, url.QueryEscape(*database))
	} else {
		connString = *dsn
	}

	if *debug {
		fmt.Printf(" dsn:      %s\n", connString)
	}

	conn, err := sql.Open(azuread.DriverName, connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select 1, 'abc', suser_name()")
	if err != nil {
		log.Fatal("Prepare failed:", err.Error())
	}
	defer stmt.Close()

	row := stmt.QueryRow()
	var somenumber int64
	var somechars string
	var someuser string
	err = row.Scan(&somenumber, &somechars, &someuser)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}
	fmt.Printf("number: %d\n", somenumber)
	fmt.Printf("chars:  %s\n", somechars)
	fmt.Printf("user:   %s\n", someuser)

	fmt.Printf("bye\n")
}
