//go:build go1.10
// +build go1.10

package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/golang-sql/sqlexp"
	mssql "github.com/microsoft/go-mssqldb"
)

const (
	msgQuery = `select 'name' as Name
PRINT N'This is a message'
select 199
RAISERROR (N'Testing!' , 11, 1)
select 300
`
)

// This example shows the usage of sqlexp/Messages
func ExampleRows_usingmessages() {

	connString := makeConnURL().String()

	// Create a new connector object by calling NewConnector
	connector, err := mssql.NewConnector(connString)
	if err != nil {
		log.Println(err)
		return
	}

	// Pass connector to sql.OpenDB to get a sql.DB object
	db := sql.OpenDB(connector)
	defer db.Close()
	retmsg := &sqlexp.ReturnMessage{}
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, msgQuery, retmsg)
	if err != nil {
		log.Fatalf("QueryContext failed: %v", err)
	}
	active := true
	for active {
		msg := retmsg.Message(ctx)
		switch m := msg.(type) {
		case sqlexp.MsgNotice:
			fmt.Println(m.Message)
		case sqlexp.MsgNext:
			inresult := true
			for inresult {
				inresult = rows.Next()
				if inresult {
					cols, err := rows.Columns()
					if err != nil {
						log.Fatalf("Columns failed: %v", err)
					}
					fmt.Println(cols)
					var d interface{}
					if err = rows.Scan(&d); err == nil {
						fmt.Println(d)
					}
				}
			}
		case sqlexp.MsgNextResultSet:
			active = rows.NextResultSet()
		case sqlexp.MsgError:
			fmt.Println("Error:", m.Error)
		case sqlexp.MsgRowsAffected:
			fmt.Println("Rows affected:", m.Count)
		}
	}
}
