package mssql_test

import "fmt"

func ExampleError_SQLErrorNumber() {
	// call a function that might return a mssql error
	err := callUsingMSSQL()

	type ErrorWithNumber interface {
		SQLErrorNumber() int32
	}

	if errorWithNumber, ok := err.(ErrorWithNumber); ok {
		if errorWithNumber.SQLErrorNumber() == 1205 {
			fmt.Println("deadlock error")
		}
	}
}

func ExampleError_SQLErrorMessage() {
	// call a function that might return a mssql error
	err := callUsingMSSQL()

	type SQLError interface {
		SQLErrorNumber() int32
		SQLErrorMessage() string
	}

	if sqlError, ok := err.(SQLError); ok {
		if sqlError.SQLErrorNumber() == 1205 {
			fmt.Println("deadlock error", sqlError.SQLErrorMessage())
		}
	}
}

func callUsingMSSQL() error {
	return nil
}
