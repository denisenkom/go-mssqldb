package main

/*
Notes:

This demonstrates how to use the native fedauth functionality with AWS RDS Proxy for MS SQL server.
Connection string is simple as the access token is retrieved via the token provider in NewConnectorWithAccessTokenProvider.

How to use (make sure you have an active IAM user api key or role via the regular methods):
1. Create an RDS MS SQL Server (Express is fine for cheapness)
2. Create an RDS Proxy (plug in your requirements, make sure you escape any !'s in the secrets ARN)
aws rds create-db-proxy \
    --db-proxy-name <sqlproxy> \
    --engine-family SQLSERVER  \
    --auth Description="MS SQL RDS Proxy",AuthScheme="SECRETS",SecretArn="<ARN>",IAMAuth="ENABLED",ClientPasswordAuthType="SQL_SERVER_AUTHENTICATION" \
    --role-arn "<RDS PROXY Role ARN>"\
    --vpc-subnet-ids "<subnet-xxx>" "<subnet-yyy>" \
    --vpc-security-group-ids <sg-xxx> \
	--require-tls

 3. Register your RDS DB with the proxy:
aws rds register-db-proxy-targets \
    --db-proxy-name <sqlproxy> \
    --db-instance-identifiers "<sqlexpress>"

 4. Ensure your IAM User/Role allows rds-db:connect as per https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html
 5. Enter resulting Proxy FQDN below in server variable or pass via argument
*/

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	_ "github.com/microsoft/go-mssqldb"
	mssql "github.com/microsoft/go-mssqldb"
	"log"
	"strconv"
)

var (
	debug  = flag.Bool("debug", false, "enable debugging")
	server = flag.String("server", "", "the database server")
	user   = flag.String("user", "admin", "the user")
	region = flag.String("region", "ap-southeast-2", "the region")
	port   = 1433
)

func main() {
	flag.Parse()

	if *debug {
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user: %s\n", *user)
		fmt.Printf(" region: %s\n", *region)
		fmt.Printf(" port: %d\n", port)
	}

	if *server == "" {
		log.Fatal("Server name cannot be left empty")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	endpoint := *server + ":" + strconv.Itoa(port)
	connString := fmt.Sprintf("server=%s;port=%d;",
		*server, port)
	tokenProviderWithCtx := func(ctx context.Context) (string, error) {
		authToken, err := auth.BuildAuthToken(
			context.TODO(),
			endpoint,
			*region,
			*user,
			cfg.Credentials)
		if err != nil {
			log.Fatal("Open connection failed:", err.Error())
		}
		return authToken, nil
	}

	connector, err := mssql.NewConnectorWithAccessTokenProvider(connString, tokenProviderWithCtx)
	conn := sql.OpenDB(connector)

	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	fmt.Printf("Connected!\n")
	defer conn.Close()

	stmt, err := conn.Prepare("select @@version as version")
	if err != nil {
		log.Fatal("Error preparing SQL statement:", err.Error())
	}
	row := stmt.QueryRow()

	var result string

	err = row.Scan(&result)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}

	fmt.Printf("%s\n", result)
}
