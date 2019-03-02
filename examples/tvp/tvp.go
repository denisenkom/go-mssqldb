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

type TvpExample struct {
	Message              string
	OmitField            string  `skip:"-"`
	OmitWrongTypingField []*byte `skip:"-"`
}

const (
	crateSchema = `create schema TestTVPSchema;`

	dropSchema = `drop schema TestTVPSchema;`

	createTVP = `
		CREATE TYPE TestTVPSchema.exampleTVP AS TABLE
		(
			message	NVARCHAR(100)
		)`

	dropTVP = `DROP TYPE TestTVPSchema.exampleTVP;`

	procedureWithTVP = `	
	CREATE PROCEDURE ExecTVP
		@param1 TestTVPSchema.exampleTVP READONLY
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

	exampleData := []TvpExample{
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
		TVPTypeName:  "exampleTVP",
		TVPScheme:    "TestTVPSchema",
		TVPValue:     exampleData,
		TVPCustomTag: "skip",
	}

	rows, err := conn.Query(execTvp,
		sql.Named("param1", tvpType),
	)
	if err != nil {
		log.Println(err)
		return
	}

	tvpResult := make([]TvpExample, 0)
	for rows.Next() {
		tvpExample := TvpExample{}
		err = rows.Scan(&tvpExample.Message)
		if err != nil {
			log.Println(err)
			return
		}
		tvpResult = append(tvpResult, tvpExample)
	}
	fmt.Println(tvpResult)
}
