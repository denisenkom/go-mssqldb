package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/denisenkom/go-mssqldb"
	"log"
)

var (
	debug         = flag.Bool("debug", false, "enable debugging")
	password      = flag.String("password", "", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "", "the database server")
	user          = flag.String("user", "", "the database user")
)

type TvpExemple struct {
	Message string
}

const (
	crateSchema = `create schema TestTVPSchema;`

	dropSchema = `drop schema TestTVPSchema;`

	createTVP = `
		CREATE TYPE TestTVPSchema.exempleTVP AS TABLE
		(
			message	NVARCHAR(100)
		)`

	dropTVP = `DROP TYPE TestTVPSchema.exempleTVP;`

	procedureWithTVP = `	
	CREATE PROCEDURE ExecTVP
		@param1 TestTVPSchema.exempleTVP READONLY
	AS   
	BEGIN
		SET NOCOUNT ON; 
		SELECT * FROM @param1;
	END;
	`

	dropProcedure = `drop PROCEDURE ExecTVP`

	execTvp = `exec ExecTVP @param1;`
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
	conn, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	_, err = conn.Exec(crateSchema)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Exec(dropSchema)

	_, err = conn.Exec(createTVP)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Exec(dropTVP)

	_, err = conn.Exec(procedureWithTVP)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Exec(dropProcedure)

	exempleData := []TvpExemple{
		{
			Message: "Hello",
		},
		{
			Message: "World",
		},
		{
			Message: "TVP",
		},
	}

	tvpType := mssql.TVPType{
		TVPTypeName: "exempleTVP",
		TVPScheme:   "TestTVPSchema",
		TVPValue:    exempleData,
	}

	rows, err := conn.Query(execTvp,
		sql.Named("param1", tvpType),
	)
	if err != nil {
		log.Println(err)
		return
	}

	tvpResult := make([]TvpExemple, 0)
	for rows.Next() {
		tvpExemple := TvpExemple{}
		err = rows.Scan(&tvpExemple.Message)
		if err != nil {
			log.Println(err)
			return
		}
		tvpResult = append(tvpResult, tvpExemple)
	}
	fmt.Println(tvpResult)
}
