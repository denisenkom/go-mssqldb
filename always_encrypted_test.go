package mssql

import (
	"fmt"
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

type testAEStruct struct {
	Id int
	SSN string
	Date time.Time
	Float *float64
	Money *float64
}

func TestAlwaysEncrypted(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	rows, err := conn.Query("SELECT id, ssn, secure_date, secure_float, secure_money FROM [dbo].[cid]")
	defer rows.Close()

	if err != nil {
		t.Fatalf("unable to query db: %s", err)
	}

	var dest testAEStruct

	expectedValues := []string{
		"12345     ",
		"00000     ",
		"041-64-841",
		"009-34-870",
		"517-04-462",
		"158-16-318",
		"136-01-843",
	}

	secureFloat := []float64{
		1.0,
		453.32,
	}

	secureMoney := []float64{
		40333.95,
		8284323.0,
	}


	expectedSecureFloat := []*float64 {
		&secureFloat[0],
		&secureFloat[1],
		nil,
		nil,
		nil,
		nil,
		nil,
	}

	expectedSecureMoney := []*float64 {
		&secureMoney[0],
		&secureMoney[1],
		nil,
		nil,
		nil,
		nil,
		nil,
	}

	expectedDate := time.Date(2021, 02, 11, 0, 0, 0, 0, time.UTC)
	expectedIdx := 0

	for rows.Next() {
		err = rows.Scan(&dest.Id, &dest.SSN, &dest.Date, &dest.Float, &dest.Money)
		fmt.Printf("col: %+v", dest)
		if dest.Float != nil {
			fmt.Printf("\t%f", *dest.Float)
		}

		if dest.Money != nil {
			fmt.Printf("\t%f", *dest.Money)
		}
		fmt.Printf("\n")

		assert.Equal(t, expectedValues[expectedIdx], dest.SSN)
		assert.Equal(t, expectedDate, dest.Date.UTC())
		checkNilandValue(t, expectedSecureFloat, expectedIdx, dest.Float)
		checkNilandValue(t, expectedSecureMoney, expectedIdx, dest.Money)


		expectedIdx++
		assert.Nil(t, err)
	}
}

func checkNilandValue(t *testing.T, expectedArr []*float64, expectedIdx int, res *float64) {
	if expectedArr[expectedIdx] == nil {
		assert.Nil(t, res)
	} else {
		assert.NotNil(t, res)
		assert.Equal(t, *expectedArr[expectedIdx], *res)
	}
}