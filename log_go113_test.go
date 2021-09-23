// +build go1.13

package mssql

import (
	"io"
	"log"
)

func currentLogWriter() io.Writer {
	return log.Writer()
}
