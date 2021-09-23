// +build !go1.13

package mssql

import (
	"io"
	"os"
)

func currentLogWriter() io.Writer {
	// There is no function for getting the current writer in versions of
	// Go older than 1.13, so we just return the default writer.
	return os.Stderr
}
