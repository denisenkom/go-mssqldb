// +build go1.10

package mssql_test

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/denisenkom/go-mssqldb"
)

// This example shows how to use tvp type
func ExampleTVPType() {
	const (
		createTable = "CREATE TABLE Location (Name VARCHAR(50), CostRate INT, Availability BIT, ModifiedDate DATETIME2)"

		dropTable = "IF OBJECT_ID('Location', 'U') IS NOT NULL DROP TABLE Location"

		createTVP = `CREATE TYPE LocationTableType AS TABLE
		(LocationName VARCHAR(50),
		CostRate INT)`

		dropTVP = "IF type_id('LocationTableType') IS NOT NULL DROP TYPE LocationTableType"

		createProc = `CREATE PROCEDURE dbo.usp_InsertProductionLocation
		@TVP LocationTableType READONLY
		AS
		SET NOCOUNT ON
		INSERT INTO Location
		(
			Name,
			CostRate,
			Availability,
			ModifiedDate)
			SELECT *, 0,GETDATE()
			FROM @TVP`

		dropProc = "IF OBJECT_ID('dbo.usp_InsertProductionLocation', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_InsertProductionLocation"

		execTvp = "exec dbo.usp_InsertProductionLocation @TVP;"
	)
	type LocationTableTvp struct {
		LocationName string
		CostRate     int64
	}

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

	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(createTVP)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Exec(dropTVP)
	_, err = db.Exec(createProc)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Exec(dropProc)

	locationTableTypeData := []LocationTableTvp{
		{
			LocationName: "Alberta",
			CostRate:     0,
		},
		{
			LocationName: "British Columbia",
			CostRate:     1,
		},
	}

	tvpType := mssql.TVPType{
		TVPTypeName: "LocationTableType",
		TVPScheme:   "",
		TVPValue:    locationTableTypeData,
	}

	_, err = db.Exec(execTvp, sql.Named("TVP", tvpType))
	if err != nil {
		log.Fatal(err)
	}
}
