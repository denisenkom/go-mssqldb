package main

import (
	"fmt"
	"database/sql"
	_ "github.com/mattn/go-adodb"
	"os"
)

func main() {
	os.Remove("example.csv")

	db, err := sql.Open("adodb", `Provider=Microsoft.Jet.OLEDB.4.0;Data Source=.;Extended Properties="Text;HDR=NO;FMT=Delimited"`)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = db.Exec("create table example.csv(f1 text, f2 text, f3 text)")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return
	}
	stmt, err := tx.Prepare("insert into example.csv(F1, F2, F3) values(?, ?, ?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 100; i++ {
		_, err = stmt.Exec(
			i,
			fmt.Sprintf("HelloWorld%03d", i),
			fmt.Sprintf("こんにちわ世界%03d", i))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	tx.Commit()

	rows, err := db.Query("select F1, F2, F3 from example.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var f1, f2, f3 string
		rows.Scan(&f1, &f2, &f3)
		fmt.Println(f1, f2, f3)
	}
}
