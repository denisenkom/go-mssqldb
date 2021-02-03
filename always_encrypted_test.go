package mssql

import (
	"fmt"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestAlwaysEncrypted(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	rows, err := conn.Query("SELECT id, ssn FROM [dbo].[cid]")
	defer rows.Close()

	if err != nil {
		t.Fatalf("unable to query db: %s", err)
	}

	var dest struct {
		Id int
		SSN string
	}

	expectedValues := []string{"12345", "00000"}
	expectedIdx := 0

	for ; rows.Next() ; {
		err = rows.Scan(&dest.Id, &dest.SSN)
		assert.Equal(t, expectedValues[expectedIdx], dest.SSN)
		expectedIdx++
		assert.Nil(t, err)
		fmt.Printf("col: %v\n", dest)
	}
}