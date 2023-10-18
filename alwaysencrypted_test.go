package mssql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/golang-sql/civil"
	"github.com/microsoft/go-mssqldb/aecmk"
	"github.com/stretchr/testify/assert"
)

type providerTest interface {
	// ProvisionMasterKey creates a master key in the key storage and returns the path of the key
	ProvisionMasterKey(t *testing.T) string
	// DeleteMasterKey deletes the master key
	DeleteMasterKey(t *testing.T)
	// GetProvider returns the appropriate ColumnEncryptionKeyProvider instance
	GetProvider(t *testing.T) aecmk.ColumnEncryptionKeyProvider
	// Name is the name of the key provider
	Name() string
}

var providerTests []providerTest = make([]providerTest, 0, 2)

func addProviderTest(p providerTest) {
	providerTests = append(providerTests, p)
}

// Define phrases for create table for each enryptable data type along with sample data for insertion and validation
type aeColumnInfo struct {
	queryPhrase string
	sqlDataType string
	encType     ColumnEncryptionType
	sampleValue interface{}
}

func TestAlwaysEncryptedE2E(t *testing.T) {
	params := testConnParams(t)
	if !params.ColumnEncryption {
		t.Skip("Test is not running with column encryption enabled")
	}
	// civil.DateTime has 9 digit precision while SQL only has 7, so we can't use time.Now
	dt, err := time.Parse("2006-01-02T15:04:05.9999999", "2023-08-21T18:33:36.5315137")
	assert.NoError(t, err, "time.Parse")
	encryptableColumns := []aeColumnInfo{
		{"int", "INT", ColumnEncryptionDeterministic, int32(1)},
		{"nchar(10) COLLATE Latin1_General_BIN2", "NCHAR", ColumnEncryptionDeterministic, NChar("ncharval")},
		{"tinyint", "TINYINT", ColumnEncryptionRandomized, byte(2)},
		{"smallint", "SMALLINT", ColumnEncryptionDeterministic, int16(-3)},
		{"bigint", "BIGINT", ColumnEncryptionRandomized, int64(4)},
		// We can't use fractional float/real values due to rounding errors in the round trip
		{"real", "REAL", ColumnEncryptionDeterministic, float32(5)},
		{"float", "FLOAT", ColumnEncryptionRandomized, float64(6)},
		{"varbinary(10)", "VARBINARY", ColumnEncryptionDeterministic, []byte{1, 2, 3, 4}},
		// TODO: Varchar support requires proper selection of a collation and conversion
		// {"varchar(10) COLLATE Latin1_General_BIN2", "VARCHAR", ColumnEncryptionRandomized, VarChar("varcharval")},
		{"nvarchar(30)", "NVARCHAR", ColumnEncryptionRandomized, "nvarcharval"},
		{"bit", "BIT", ColumnEncryptionDeterministic, true},
		{"datetimeoffset(7)", "DATETIMEOFFSET", ColumnEncryptionRandomized, dt},
		{"datetime2(7)", "DATETIME2", ColumnEncryptionDeterministic, civil.DateTimeOf(dt)},
		{"nvarchar(max)", "NVARCHAR", ColumnEncryptionRandomized, NVarCharMax("nvarcharmaxval")},
		// TODO: The driver throws away type information about Valuer implementations and sends nil as nvarchar(1). Fix that.
		// {"int", "INT", ColumnEncryptionDeterministic, sql.NullInt32{Valid: false}},
	}
	for _, test := range providerTests {
		// turn off key caching
		aecmk.ColumnEncryptionKeyLifetime = 0
		t.Run(test.Name(), func(t *testing.T) {
			conn, _ := open(t)
			defer conn.Close()
			certPath := test.ProvisionMasterKey(t)
			defer test.DeleteMasterKey(t)
			s := fmt.Sprintf(createColumnMasterKey, certPath, test.Name(), certPath)
			if _, err := conn.Exec(s); err != nil {
				t.Fatalf("Unable to create CMK: %s", err.Error())
			}
			defer func() {
				_, err := conn.Exec(fmt.Sprintf(dropColumnMasterKey, certPath))
				assert.NoError(t, err, "dropColumnMasterKey")
			}()
			r, _ := rand.Int(rand.Reader, big.NewInt(1000))
			cekName := fmt.Sprintf("mssqlCek%d", r.Int64())
			tableName := fmt.Sprintf("mssqlAe%d", r.Int64())
			keyBytes := make([]byte, 32)
			_, _ = rand.Read(keyBytes)
			encryptedCek, err := test.GetProvider(t).EncryptColumnEncryptionKey(context.Background(), certPath, KeyEncryptionAlgorithm, keyBytes)
			assert.NoError(t, err, "Encrypt")
			createCek := fmt.Sprintf(createColumnEncryptionKey, cekName, certPath, encryptedCek)
			_, err = conn.Exec(createCek)
			assert.NoError(t, err, "Unable to create CEK")
			defer func() {
				_, err := conn.Exec(fmt.Sprintf(dropColumnEncryptionKey, cekName))
				assert.NoError(t, err, "dropColumnEncryptionKey")
			}()
			_, _ = conn.Exec("DROP TABLE IF EXISTS " + tableName)
			query := new(strings.Builder)
			insert := new(strings.Builder)
			sel := new(strings.Builder)
			_, _ = query.WriteString(fmt.Sprintf("CREATE TABLE [%s] (", tableName))
			_, _ = insert.WriteString(fmt.Sprintf("INSERT INTO [%s] VALUES (", tableName))
			_, _ = sel.WriteString("select top(1) ")
			insertArgs := make([]interface{}, len(encryptableColumns)+1)
			for i, ec := range encryptableColumns {
				encType := "RANDOMIZED"
				null := ""
				_, ok := ec.sampleValue.(sql.NullInt32)
				if ok {
					null = "NULL"
				}
				if ec.encType == ColumnEncryptionDeterministic {
					encType = "DETERMINISTIC"
				}
				_, _ = query.WriteString(fmt.Sprintf(`col%d %s ENCRYPTED WITH (ENCRYPTION_TYPE = %s,
			ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256',
			COLUMN_ENCRYPTION_KEY = [%s]) %s,
			`, i, ec.queryPhrase, encType, cekName, null))

				insertArgs[i] = ec.sampleValue
				insert.WriteString(fmt.Sprintf("@p%d,", i+1))
				sel.WriteString(fmt.Sprintf("col%d,", i))
			}
			_, _ = query.WriteString("unencryptedcolumn nvarchar(100)")
			_, _ = query.WriteString(")")
			insertArgs[len(encryptableColumns)] = "unencryptedvalue"
			insert.WriteString(fmt.Sprintf("@p%d)", len(encryptableColumns)+1))
			sel.WriteString(fmt.Sprintf("unencryptedcolumn from [%s]", tableName))
			_, err = conn.Exec(query.String())
			assert.NoError(t, err, "Failed to create encrypted table")
			defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS " + tableName) }()
			_, err = conn.Exec(insert.String(), insertArgs...)
			assert.NoError(t, err, "Failed to insert row in encrypted table")
			rows, err := conn.Query(sel.String())
			assert.NoErrorf(t, err, "Unable to query encrypted columns")
			if !rows.Next() {
				rows.Close()
				assert.FailNow(t, "rows.Next returned false")
			}
			cols, err := rows.ColumnTypes()
			assert.NoError(t, err, "rows.ColumnTypes failed")
			for i := range encryptableColumns {
				assert.Equalf(t, encryptableColumns[i].sqlDataType, cols[i].DatabaseTypeName(),
					"Got wrong type name for col%d.", i)
			}

			var unencryptedColumnValue string
			scanValues := make([]interface{}, len(encryptableColumns)+1)
			for v := range scanValues {
				if v < len(encryptableColumns) {
					scanValues[v] = new(interface{})
				}
			}
			scanValues[len(encryptableColumns)] = &unencryptedColumnValue
			err = rows.Scan(scanValues...)
			defer rows.Close()
			if err != nil {
				assert.FailNow(t, "Scan failed ", err)
			}
			for i := range encryptableColumns {
				var strVal string
				var expectedStrVal string
				if encryptableColumns[i].sampleValue == nil {
					expectedStrVal = "NULL"
				} else {
					expectedStrVal = comparisonValueFromObject(encryptableColumns[i].sampleValue)
				}
				rawVal := scanValues[i].(*interface{})

				if rawVal == nil {
					strVal = "NULL"
				} else {
					strVal = comparisonValueFromObject(*rawVal)
				}
				assert.Equalf(t, expectedStrVal, strVal, "Incorrect value for col%d. ", i)
			}
			assert.Equalf(t, "unencryptedvalue", unencryptedColumnValue, "Got wrong value for unencrypted column")
			_ = rows.Next()
			err = rows.Err()
			assert.NoError(t, err, "rows.Err() has non-nil values")
			testProviderErrorHandling(t, test.Name(), test.GetProvider(t), sel.String(), insert.String(), insertArgs)
		})
	}
}

func testProviderErrorHandling(t *testing.T, name string, provider aecmk.ColumnEncryptionKeyProvider, sel string, insert string, insertArgs []interface{}) {
	t.Helper()
	testProvider := &testKeyProvider{fallback: provider}
	connector, _ := getTestConnector(t)
	connector.RegisterCekProvider(name, testProvider)
	conn := sql.OpenDB(connector)
	defer conn.Close()
	testProvider.decrypt = func(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, cek []byte) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Hour))
	defer cancel()
	rows, err := conn.QueryContext(ctx, sel)
	defer rows.Close()

	if assert.NoError(t, err, "Exec should return no error") {
		if rows.Next() {
			assert.Fail(t, "rows.Next should have failed")
		}
		assert.ErrorIs(t, rows.Err(), context.DeadlineExceeded)
	}

	var notAllowed error
	testProvider.decrypt = func(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, cek []byte) ([]byte, error) {
		notAllowed = aecmk.KeyPathNotAllowed(masterKeyPath, aecmk.Decryption)
		return nil, notAllowed
	}
	_, err = conn.Exec(insert, insertArgs...)
	assert.ErrorIs(t, err, notAllowed, "Insert should fail with key path not allowed")

}

func comparisonValueFromObject(object interface{}) string {
	switch v := object.(type) {
	case []byte:
		{
			return string(v)
		}
	case string:
		return v
	case time.Time:
		return civil.DateTimeOf(v).String()
		//return v.Format(time.RFC3339)
	case fmt.Stringer:
		return v.String()
	case bool:
		if v == true {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

const (
	createColumnMasterKey     = `CREATE COLUMN MASTER KEY [%s] WITH (KEY_STORE_PROVIDER_NAME= '%s', KEY_PATH='%s')`
	dropColumnMasterKey       = `DROP COLUMN MASTER KEY [%s]`
	createColumnEncryptionKey = `CREATE COLUMN ENCRYPTION KEY [%s] WITH VALUES (COLUMN_MASTER_KEY = [%s], ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x%x )`
	dropColumnEncryptionKey   = `DROP COLUMN ENCRYPTION KEY [%s]`
	createEncryptedTable      = `CREATE TABLE %s 
	    (col1 int 
			ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC,
							ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256',
							COLUMN_ENCRYPTION_KEY = [%s]),
		col2 nchar(10) COLLATE Latin1_General_BIN2
			ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC,
				ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256',
				COLUMN_ENCRYPTION_KEY = [%s])
		)`
)

// Parameterized implementation of a key provider
type testKeyProvider struct {
	encrypt  func(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, cek []byte) ([]byte, error)
	decrypt  func(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, encryptedCek []byte) ([]byte, error)
	lifetime *time.Duration
	fallback aecmk.ColumnEncryptionKeyProvider
}

func (p *testKeyProvider) DecryptColumnEncryptionKey(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, encryptedCek []byte) (decryptedKey []byte, err error) {
	if p.decrypt != nil {
		return p.decrypt(ctx, masterKeyPath, encryptionAlgorithm, encryptedCek)
	}
	return p.fallback.DecryptColumnEncryptionKey(ctx, masterKeyPath, encryptionAlgorithm, encryptedCek)
}

func (p *testKeyProvider) EncryptColumnEncryptionKey(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, cek []byte) ([]byte, error) {
	if p.encrypt != nil {
		return p.encrypt(ctx, masterKeyPath, encryptionAlgorithm, cek)
	}
	return p.fallback.EncryptColumnEncryptionKey(ctx, masterKeyPath, encryptionAlgorithm, cek)
}

func (p *testKeyProvider) SignColumnMasterKeyMetadata(ctx context.Context, masterKeyPath string, allowEnclaveComputations bool) ([]byte, error) {
	return nil, nil
}

// VerifyColumnMasterKeyMetadata verifies the specified signature is valid for the column master key
// with the specified key path and the specified enclave behavior. Return nil if not supported.
func (p *testKeyProvider) VerifyColumnMasterKeyMetadata(ctx context.Context, masterKeyPath string, allowEnclaveComputations bool) (*bool, error) {
	return nil, nil
}

// KeyLifetime is an optional Duration. Keys fetched by this provider will be discarded after their lifetime expires.
// If it returns nil, the keys will expire based on the value of ColumnEncryptionKeyLifetime.
// If it returns zero, the keys will not be cached.
func (p *testKeyProvider) KeyLifetime() *time.Duration {
	if p.lifetime != nil {
		return p.lifetime
	}
	return p.fallback.KeyLifetime()
}
