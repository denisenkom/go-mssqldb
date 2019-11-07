package mssql_test

import (
	"flag"
	"fmt"
	"log"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
)

var (
	password      = flag.String("password", "", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "", "the database server")
	user          = flag.String("user", "", "the database user")
	database      = flag.String("database", "", "the database name")
)

// This example shows the how to set query notifications on a pre-existing table
func ExampleStmt_SetQueryNotification() {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;", *server, *user, *password, *port, *database)

	mssqldriver := &mssql.Driver{}
	cn, err := mssqldriver.Open(connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer cn.Close()
	conn, _ := cn.(*mssql.Conn)

	// Supported SELECT statements: https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms181122(v=sql.105)
	stmt, err := conn.Prepare("SELECT [myColumn] FROM [mySchema].[myTable];")
	if err != nil {
		log.Fatal("Prepare failed:", err.Error())
	}
	defer stmt.Close()

	sqlstmt, _ := stmt.(*mssql.Stmt)
	defer sqlstmt.Close()
	sqlstmt.SetQueryNotification("Message", "service=myService", time.Hour)

	// Query will return the result of the above select statement and subscription for the query notification will be created.
	rows, err := sqlstmt.Query(nil)
	if err != nil {
		log.Fatal("Query failed:", err.Error())
	} else {
		rows.Close()
	}
}
