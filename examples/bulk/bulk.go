package main

import "github.com/sqlserverio/go-mssqldb"
import "database/sql"
import "log"
import "fmt"
import "flag"

var debug = flag.Bool("debug", true, "enable debugging")
var password = flag.String("password", "", "the database password")
var port *int = flag.Int("port", 1433, "the database port")
var server = flag.String("server", "", "the database server")
var user = flag.String("user", "", "the database user")
var database = flag.String("database", "", "the database name")

/**
	CREATE TABLE table_test(
		[id] [int] IDENTITY(1,1) NOT NULL,
		[test_nvarchar] [nvarchar](50) NULL,
		[test_varchar] [varchar](50) NULL,
		[test_float] [float] NOT NULL,
		[test_datetime2_3] [datetime2](3) NULL,
		[test_bitn] [bit] NULL,
		[test_bigint] [bigint] NOT NULL,
		[test_geom] [geometry] NULL,
	 CONSTRAINT [PK_table_test_id] PRIMARY KEY CLUSTERED
	(
		[id] ASC
	) ON [PRIMARY]);
**/

func main() {
	flag.Parse() // parse the command line args

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user:%s\n", *user)
		fmt.Printf(" database:%s\n", *database)
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", *server, *user, *password, *port, *database)
	if *debug {
		fmt.Printf("connString:%s\n", connString)
	}
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	txn, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(mssql.CopyIn("test_table", "test_maxvchar", "test_int"))
	if err != nil {
		log.Fatal(err.Error())
	}

	for i := 0; i < 10; i++ {
		_, err = stmt.Exec(generateStringUnicode(0, 30), i)
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
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	var str string
	for len(str) < n {
		str = str + alphabet
	}

	return str + "$"
}
func generateStringUnicode(x int, n int) string {
	alphabet := "ab©ĎéⒻghïjklmnopqЯ☀tuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	startPos := x % 26
	if startPos+n > len(alphabet) {
		alphabet = alphabet + alphabet
	}
	return alphabet[startPos:(startPos+n)-1] + "$"
}
