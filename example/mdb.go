package main

import (
	"exp/sql"
	"fmt"
	_ "github.com/mattn/go-adodb"
	"os"
)

func main() {
	if _, err := os.Stat("./example.mdb"); err != nil {
		fmt.Println("put here empty database named 'example.mdb'.")
		return
	}

	db, err := sql.Open("adodb", "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=./example.mdb;")
	if err != nil {
		fmt.Println(err)
		return
	}

	sqls := []string{
		"drop table foo",
		"create table foo (id int not null primary key, name text)",
	}
	for _, sql := range sqls {
		_, err = db.Exec(sql)
		if err != nil {
			fmt.Printf("%q: %s\n", err, sql)
			return
		}
	}

	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return
	}
	stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("こんにちわ世界%03d", i))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	tx.Commit()

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		println(id, name)
	}
}
