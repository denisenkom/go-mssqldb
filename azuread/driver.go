package azuread

import (
	"context"
	"database/sql"
	"database/sql/driver"

	mssql "github.com/denisenkom/go-mssqldb"
)

// DriverName is the name used to register the driver
const DriverName = "azuresql"

func init() {
	sql.Register(DriverName, &Driver{})
}

// Driver wraps the underlying MSSQL driver, but configures the Azure AD token provider
type Driver struct {
}

// Open returns a new connection to the database.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

// NewConnector creates a new connector from a DSN.
// The returned connector may be used with sql.OpenDB.
func NewConnector(dsn string) (*mssql.Connector, error) {
	params, err := splitConnectionStringURL(dsn)
	if err != nil {
		return nil, err
	}

	config, err := validateParameters(params)
	if err != nil {
		return nil, err
	}

	switch config.fedAuthLibrary {
	case fedAuthLibrarySecurityToken:
		return mssql.NewSecurityTokenConnector(
			dsn,
			func(ctx context.Context) (string, error) {
				return config.provideSecurityToken(ctx)
			},
		)

	case fedAuthLibraryADAL:
		return mssql.NewActiveDirectoryTokenConnector(
			dsn, config.adalWorkflow,
			func(ctx context.Context, serverSPN, stsURL string) (string, error) {
				return config.provideActiveDirectoryToken(ctx, serverSPN, stsURL)
			},
		)

	default:
		return mssql.NewConnector(dsn)
	}
}
