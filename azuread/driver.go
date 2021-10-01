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

	config, err := parse(dsn)
	if err != nil {
		return nil, err
	}
	return newConnectorConfig(config)
}

// newConnectorConfig creates a Connector from config.
func newConnectorConfig(config *azureFedAuthConfig) (*mssql.Connector, error) {
	if config.fedAuthLibrary == mssql.FedAuthLibraryADAL {
		return mssql.NewActiveDirectoryTokenConnector(
			config.mssqlConfig, config.adalWorkflow,
			func(ctx context.Context, serverSPN, stsURL string) (string, error) {
				return config.provideActiveDirectoryToken(ctx, serverSPN, stsURL)
			},
		)
	}
	return mssql.NewConnectorConfig(config.mssqlConfig), nil
}
