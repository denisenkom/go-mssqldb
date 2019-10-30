package mssql_test

import (
	"flag"
	"fmt"
	"log"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
)

var (
	database = flag.String("database", "", "the database name")
)

// This example shows the how to set query notifications
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

	rows, err := sqlstmt.Query(nil)
	if err != nil {
		log.Fatal("Query failed:", err.Error())
	} else {
		rows.Close()
	}
}
