//go:build go1.17
// +build go1.17

package mssql

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/golang-sql/civil"
	"github.com/microsoft/go-mssqldb/aecmk/localcert"
	"github.com/microsoft/go-mssqldb/internal/certs"
)

// Define phrases for create table for each enryptable data type along with sample data for insertion and validation
type aeColumnInfo struct {
	queryPhrase string
	sqlDataType string
	encType     ColumnEncryptionType
	sampleValue interface{}
}

var encryptableColumns = []aeColumnInfo{
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
	{"datetimeoffset(7)", "DATETIMEOFFSET", ColumnEncryptionRandomized, time.Now()},
	{"datetime2(7)", "DATETIME2", ColumnEncryptionDeterministic, civil.DateTimeOf(time.Now())},
	{"nvarchar(max)", "NVARCHAR", ColumnEncryptionRandomized, NVarCharMax("nvarcharmaxval")},
	// TODO: The driver throws away type information about Valuer implementations and sends nil as nvarchar(1). Fix that.
	// {"int", "INT", ColumnEncryptionDeterministic, sql.NullInt32{Valid: false}},
}

func TestAlwaysEncryptedE2E(t *testing.T) {
	params := testConnParams(t)
	if !params.ColumnEncryption {
		t.Skip("Test is not running with column encryption enabled")
	}
	conn, _ := open(t)
	defer conn.Close()
	thumbprint, err := certs.ProvisionMasterKeyInCertStore()
	if err != nil {
		t.Fatal(err)
	}
	defer certs.DeleteMasterKeyCert(thumbprint)
	certPath := fmt.Sprintf(`CurrentUser/My/%s`, thumbprint)
	s := fmt.Sprintf(createColumnMasterKey, certPath, certPath)
	if _, err := conn.Exec(s); err != nil {
		t.Fatalf("Unable to create CMK: %s", err.Error())
	}
	defer conn.Exec(fmt.Sprintf(dropColumnMasterKey, certPath))
	r, _ := rand.Int(rand.Reader, big.NewInt(1000))
	cekName := fmt.Sprintf("mssqlCek%d", r.Int64())
	tableName := fmt.Sprintf("mssqlAe%d", r.Int64())
	keyBytes := make([]byte, 32)
	_, _ = rand.Read(keyBytes)
	encryptedCek := localcert.WindowsCertificateStoreKeyProvider.EncryptColumnEncryptionKey(certPath, KeyEncryptionAlgorithm, keyBytes)
	createCek := fmt.Sprintf(createColumnEncryptionKey, cekName, certPath, encryptedCek)
	_, err = conn.Exec(createCek)
	if err != nil {
		t.Fatalf("Unable to create CEK: %s", err.Error())
	}
	defer conn.Exec(fmt.Sprintf(dropColumnEncryptionKey, cekName))
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
	if err != nil {
		t.Fatalf("Failed to create encrypted table %s", err.Error())
	}
	defer conn.Exec("DROP TABLE IF EXISTS " + tableName)
	_, err = conn.Exec(insert.String(), insertArgs...)
	if err != nil {
		t.Fatalf("Failed to insert row in encrypted table %s", err.Error())
	}
	rows, err := conn.Query(sel.String())
	if err != nil {
		t.Fatalf("Unable to query encrypted columns: %v", err.(Error).All)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatalf("rows.Next returned false")
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("rows.ColumnTypes failed %s", err.Error())
	}
	for i := range encryptableColumns {

		if cols[i].DatabaseTypeName() != encryptableColumns[i].sqlDataType {
			t.Fatalf("Got wrong type name for col%d. Expected: %s, Got:%s", i, encryptableColumns[i].sqlDataType, cols[i].DatabaseTypeName())
		}
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
	if err != nil {
		rows.Close()
		t.Fatalf("rows.Scan failed: %s", err.Error())
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
		if expectedStrVal != strVal {
			t.Fatalf("Incorrect value for col%d. Expected:%s, Got:%s", i, expectedStrVal, strVal)
		}
	}
	if unencryptedColumnValue != "unencryptedvalue" {
		t.Fatalf("Got wrong value for unencrypted column: %s", unencryptedColumnValue)
	}
	rows.Close()
	err = rows.Err()
	if err != nil {
		t.Fatalf("rows.Err() has non-nil value: %s", err.Error())
	}
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
	createColumnMasterKey     = `CREATE COLUMN MASTER KEY [%s] WITH (KEY_STORE_PROVIDER_NAME= 'MSSQL_CERTIFICATE_STORE', KEY_PATH='%s')`
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
