package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	debug         = flag.Bool("debug", false, "enable debugging")
	password      = flag.String("password", "", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "", "the database server")
	user          = flag.String("user", "", "the database user")
)

func main() {
	flag.Parse()

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user:%s\n", *user)
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", *server, *user, *password, *port)
	if *debug {
		fmt.Printf(" connString:%s\n", connString)
	}

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	cExec := 100

	dropSql := "drop table test"
	db.Exec(dropSql)
	createSql := "create table test (id INT, idstr varchar(10))"
	_, err = db.Exec(createSql)
	if err != nil {
		log.Fatal(err)
	}

	insertSql := "insert into test (id, idstr) values (@p1, @p2)"
	done := make(chan bool)
	stmt, err := db.Prepare(insertSql)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Stmt is safe to be used by multiple goroutines
	var wg sync.WaitGroup
	wg.Add(cExec)
	for j := 0; j < cExec; j++ {
		go func(val int) {
			defer wg.Done()
			_, err := stmt.Exec(val, strconv.Itoa(val))
			if err != nil {
				log.Fatal(err)
			}
		}(j)
	}
	wg.Wait()

	selectSql := "select idstr from test where id = "
	// DB is safe to be used by multiple goroutines
	for i := 0; i < cExec; i++ {
		go func(key int) {
			rows, err := db.Query(selectSql + strconv.Itoa(key))
			if err != nil {
				log.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				err := rows.Scan(&id)
				if err != nil {
					log.Fatal(err)
				} else {
					log.Printf("Found %d\n", key)
				}
			}
			done <- true
		}(i)
	}

	for i := 0; i < cExec; i++ {
		<-done
	}
}
