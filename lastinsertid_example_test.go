// +build go1.10

package mssql_test

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
)

// This example shows the usage of Connector type
func ExampleLastInsertId() {
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

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("create table foo (bar int identity, baz int unique);")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Exec("if object_id('foo', 'U') is not null drop table foo;")

	// Attempt to retrieve scope identity using LastInsertId
	res, err := db.Exec("insert into foo (baz) values (1)")
	if err != nil {
		log.Fatal(err)
	}
	n, err := res.LastInsertId()
	if err != nil {
		log.Print(err)
		// Gets error: LastInsertId is not supported. Please use the OUTPUT clause or add `select ID = convert(bigint, SCOPE_IDENTITY())` to the end of your query.
	}
	log.Printf("LastInsertId: %d\n", n)

	// Retrieve scope identity by adding 'select ID = convert(bigint, SCOPE_IDENTITY())' to the end of the query
	rows, err := db.Query("insert into foo (baz) values (10); select ID = convert(bigint, SCOPE_IDENTITY())")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var lastInsertId1 int64
	for rows.Next() {
		rows.Scan(&lastInsertId1)
		log.Printf("LastInsertId from SCOPE_IDENTITY(): %d\n", lastInsertId1)
	}

	// Retrieve scope identity by 'output inserted``
	var lastInsertId2 int64
	err = db.QueryRow("insert into foo (baz) output inserted.bar values (100)").Scan(&lastInsertId2)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("LastInsertId from output inserted: %d\n", lastInsertId2)
}
