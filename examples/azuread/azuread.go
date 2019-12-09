package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	debug    = flag.Bool("debug", false, "enable debugging")
	server   = flag.String("server", os.Getenv("SQL_SERVER"), "the database server name")
	port     = flag.Int("port", 1433, "the database port")
	database = flag.String("database", os.Getenv("SQL_DATABASE"), "the database name")
	user     = flag.String("user", os.Getenv("SQL_AD_ADMIN_USER"), "the AD administrator user name")
	password = flag.String("password", os.Getenv("SQL_AD_ADMIN_PASSWORD"), "the AD administrator password")
	fedauth  = flag.String("fedauth", "ActiveDirectoryPassword", "the federated authentication scheme to use")
	appName  = flag.String("app-name", os.Getenv("APP_NAME"), "the application name to authorize")
	vmName   = flag.String("vm-name", os.Getenv("VM_NAME"), "the system identity name to authorize for this VM")
	uaName   = flag.String("ua-name", os.Getenv("UA_NAME"), "the user assigned identity name to authorize for this VM")
)

func createConnStr(database string) string {
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?encrypt=true",
		url.QueryEscape(*user), url.QueryEscape(*password),
		url.QueryEscape(*server), *port)

	if database != "" && database != "master" {
		connString = connString + "&database=" + url.QueryEscape(database)
	}

	if *fedauth != "" {
		connString = connString + "&fedauth=" + url.QueryEscape(*fedauth)
	}

	if *debug {
		connString = connString + "&log=127"
	}

	return connString
}

func createDatabaseIfNotExists() error {
	// Check database exists by connecting to master on the Azure SQL server
	connString := createConnStr("master")

	log.Printf("Open: %s\n", connString)

	conn, err := sql.Open("sqlserver", connString)
	if err != nil {
		return err
	}

	defer conn.Close()

	if err = conn.Ping(); err != nil {
		return err
	}

	quoted := strings.ReplaceAll(*database, "]", "]]")
	sql := "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @p1)\n  CREATE DATABASE [" + quoted + "] ( SERVICE_OBJECTIVE = 'S0' )"
	log.Printf("Exec: @p1 = '%s'\n%s\n", *database, sql)
	_, err = conn.Exec(sql, *database)

	return err
}

func addExternalUserIfNotExists(user string) error {
	connString := createConnStr(*database)

	log.Printf("Open: %s\n", connString)

	var conn *sql.DB
	var err error

	for retry := 0; retry < 8; retry++ {
		conn, err = sql.Open("sqlserver", connString)
		if err == nil {
			if err = conn.Ping(); err == nil {
				break
			}
		}
		log.Printf("Connection failed: %v", err)
		log.Println("Retry in 15 seconds")
		time.Sleep(15 * time.Second)
	}
	if err != nil {
		log.Printf("Connection failed: %v", err)
		log.Println("No further retries will be attempted")
		return err
	}

	defer conn.Close()

	quoted := strings.ReplaceAll(user, "]", "]]")
	sql := "IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @p1)\n  CREATE USER [" + quoted + "] FROM EXTERNAL PROVIDER"
	log.Printf("Exec: @p1 = '%s'\n%s\n", user, sql)
	_, err = conn.Exec(sql, user)
	if err != nil {
		return err
	}

	sql = "IF IS_ROLEMEMBER('db_owner', @p1) = 0\n  ALTER ROLE [db_owner] ADD MEMBER [" + quoted + "]"
	log.Printf("Exec: @p1 = '%s'\n%s\n", user, sql)
	_, err = conn.Exec(sql, user)

	return err
}

func main() {
	flag.Parse()

	err := createDatabaseIfNotExists()
	if err != nil {
		log.Fatalf("Unable to create database [%s]: %v", *database, err)
	}

	if *vmName != "" {
		err = addExternalUserIfNotExists(*vmName)
		if err != nil {
			log.Fatalf("Unable to create user for system-assigned identity [%s]: %v", *vmName, err)
		}
	}

	if *appName != "" {
		err = addExternalUserIfNotExists(*appName)
		if err != nil {
			log.Fatalf("Unable to create user for application identity [%s]: %v", *appName, err)
		}
	}

	if *uaName != "" {
		err = addExternalUserIfNotExists(*uaName)
		if err != nil {
			log.Fatalf("Unable to create user for user-assigned identity [%s]: %v", *uaName, err)
		}
	}
}
