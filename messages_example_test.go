// +build go1.10

package mssql_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/golang-sql/sqlexp"
)

const (
	msgQuery = `select name from sys.tables
PRINT N'This is a message'
select 199
RAISERROR (N'Testing!' , 11, 1)
select 300
`
)

// This example shows the usage of sqlexp/Messages
func ExampleRows_usingmessages() {
	flag.Parse()

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user:%s\n", *user)
	}

	connString := makeConnURL().String()
	if *debug {
		fmt.Printf(" connString:%s\n", connString)
	}

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
				cols, err := rows.Columns()
				if err != nil {
					log.Fatalf("Columns failed: %v", err)
				}
				fmt.Println(cols)
				if inresult {
					var d interface{}
					rows.Scan(&d)
					fmt.Println(d)
				}
			}
		case sqlexp.MsgNextResultSet:
			active = rows.NextResultSet()
		case sqlexp.MsgError:
			fmt.Fprintln(os.Stderr, m.Error)
		case sqlexp.MsgRowsAffected:
			fmt.Println("Rows affected:", m.Count)
		}
	}

}
