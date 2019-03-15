package mssql_test

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	mssql "github.com/denisenkom/go-mssqldb"
)

const (
	createTestTable = `CREATE TABLE test_table(
		[id] [int] IDENTITY(1,1) NOT NULL,
		[test_nvarchar] [nvarchar](50) NULL,
		[test_varchar] [varchar](50) NULL,
		[test_float] [float] NULL,
		[test_datetime2_3] [datetime2](3) NULL,
		[test_bitn] [bit] NULL,
		[test_bigint] [bigint] NOT NULL,
		[test_geom] [geometry] NULL,
	CONSTRAINT [PK_table_test_id] PRIMARY KEY CLUSTERED
	(
		[id] ASC
	) ON [PRIMARY]);`
	dropTestTable = "IF OBJECT_ID('test_table', 'U') IS NOT NULL DROP TABLE test_table;"
)

// This example shows the usage of Connector type
func ExampleCopyIn() {
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

	conn, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	txn, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Create table
	_, err = conn.Exec(createTestTable)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Exec(dropTestTable)

	// mssqldb.CopyIn creates string to be consumed by Prepare
	stmt, err := txn.Prepare(mssql.CopyIn("test_table", mssql.BulkOptions{}, "test_varchar", "test_nvarchar", "test_float", "test_bigint"))
	if err != nil {
		log.Fatal(err.Error())
	}

	for i := 0; i < 10; i++ {
		_, err = stmt.Exec(generateString(0, 30), generateStringUnicode(0, 30), i, i)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	rowCount, _ := result.RowsAffected()
	log.Printf("%d row copied\n", rowCount)
	log.Printf("bye\n")
}

func generateString(x int, n int) string {
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i%len(letters)]
	}
	return string(b)
}
func generateStringUnicode(x int, n int) string {
	letters := "ab©💾é?ghïjklmnopqЯ☀tuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i%len(letters)]
	}
	return string(b)
}
