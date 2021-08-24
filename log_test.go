package mssql

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/denisenkom/go-mssqldb/msdsn"
)

// bufLogger implements the Logger interface for testing purposes.
// It captures the log messages in a buffer for comparison
type bufLogger struct {
	Buff *bytes.Buffer
}

func (l bufLogger) Printf(format string, v ...interface{}) {
	l.Buff.WriteString(fmt.Sprintf(format, v...))
}

func (l bufLogger) Println(v ...interface{}) {
	l.Buff.WriteString(fmt.Sprintln(v...))
}

// bufContextLogger implements the ContextLogger interface for testing purposes.
// It captures the log messages in a buffer for comparison
type bufContextLogger struct {
	Buff *bytes.Buffer
}

func (l bufContextLogger) Log(_ context.Context, _ msdsn.Log, msg string) {
	l.Buff.WriteString(msg)
}

// TestLogger tests the various options for logging
func TestLogger(t *testing.T) {

	// Record system log settings and restore them after the test
	originalWriter := currentLogWriter()
	originalFlags := log.Flags()
	defer func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
	}()

	// Set system log's writer so that we can capture the log output
	// in a buffer for comparison with expected test results
	var sysLogBuf bytes.Buffer
	log.SetOutput(&sysLogBuf)
	log.SetFlags(0)

	// Record existing go-mssqldb loggers and restore them after the test
	originalLogger := driverInstance.logger
	originaLoggerNoProcess := driverInstanceNoProcess.logger
	defer func() {
		driverInstance.SetContextLogger(originalLogger)
		driverInstanceNoProcess.SetContextLogger(originaLoggerNoProcess)
	}()

	// Buffer for capturing messages logged to our Logger/ContextLogger implementations
	var captureBuf bytes.Buffer

	// Set up a retryable error and the test cases that will exercise it
	errMsg := "Retryable Test Error"
	retryPrefix := "RETRY: "
	inErr := StreamError{Message: errMsg}

	testcases := [...]struct {
		name        string
		driver      *Driver
		logger      Logger        // only one of logger or ctxLogger should be set per test
		ctxLogger   ContextLogger // only one of logger or ctxLogger should be set per test
		flags       msdsn.Log
		expectedMsg string
	}{
		{
			name:        "mssql with no logging",
			driver:      driverInstance,
			logger:      nil,
			ctxLogger:   nil,
			flags:       0,
			expectedMsg: "",
		},
		{
			name:        "sqlserver with no logging",
			driver:      driverInstanceNoProcess,
			logger:      nil,
			ctxLogger:   nil,
			flags:       0,
			expectedMsg: "",
		},
		{
			name:        "mssql with Logger logging",
			driver:      driverInstance,
			logger:      bufLogger{&captureBuf},
			ctxLogger:   nil,
			flags:       msdsn.LogRetries,
			expectedMsg: retryPrefix + errMsg,
		},
		{
			name:        "sqlserver with Logger logging",
			driver:      driverInstanceNoProcess,
			logger:      bufLogger{&captureBuf},
			ctxLogger:   nil,
			flags:       msdsn.LogRetries,
			expectedMsg: retryPrefix + errMsg,
		},
		{
			name:        "mssql with ContextLogger logging",
			driver:      driverInstance,
			logger:      nil,
			ctxLogger:   bufContextLogger{&captureBuf},
			flags:       msdsn.LogRetries,
			expectedMsg: errMsg,
		},
		{
			name:        "sqlserver with ContextLogger logging",
			driver:      driverInstanceNoProcess,
			logger:      nil,
			ctxLogger:   bufContextLogger{&captureBuf},
			flags:       msdsn.LogRetries,
			expectedMsg: errMsg,
		},
		{
			name:        "mssql with Logger logging but no flags set",
			driver:      driverInstance,
			logger:      bufLogger{&captureBuf},
			ctxLogger:   nil,
			flags:       0,
			expectedMsg: "",
		},
		{
			name:        "sqlserver with Logger logging but no flags set",
			driver:      driverInstanceNoProcess,
			logger:      bufLogger{&captureBuf},
			ctxLogger:   nil,
			flags:       0,
			expectedMsg: "",
		},
		{
			name:        "mssql with ContextLogger logging but retry logging flag not set",
			driver:      driverInstance,
			logger:      nil,
			ctxLogger:   bufContextLogger{&captureBuf},
			flags:       msdsn.LogErrors,
			expectedMsg: "",
		},
		{
			name:        "sqlserver with ContextLogger logging but retry logging flag not set",
			driver:      driverInstanceNoProcess,
			logger:      nil,
			ctxLogger:   bufContextLogger{&captureBuf},
			flags:       msdsn.LogErrors,
			expectedMsg: "",
		},
	}

	for _, tc := range testcases {

		sysLogBuf.Reset()
		captureBuf.Reset()

		// Set the logger
		if tc.logger != nil {
			SetLogger(tc.logger)
		} else if tc.ctxLogger != nil {
			SetContextLogger(tc.ctxLogger)
		} else {
			SetContextLogger(nil)
		}

		// Set up a mock connection for the test. Normally these values
		// would be set in the mssql.connect function, but that function
		// doesn't provide a ready mechanism for mocking the connection,
		// so we set the fields directly here.
		c := Conn{
			connector: &Connector{
				params: msdsn.Config{
					DisableRetry: false,
					LogFlags:     tc.flags,
				},
			},
			sess: &tdsSession{
				logger:   tc.driver.logger,
				logFlags: uint64(tc.flags),
			},
			connectionGood: true,
		}

		// Induce a retry
		outErr := c.checkBadConn(context.Background(), inErr, true)

		// Ensure that we exercised the retryable error path
		if outErr != newRetryableError(inErr) {
			t.Fatalf("checkBadConn did not return retryable error for driver '%s'. Expected '%+v', Got '%+v'",
				tc.name, newRetryableError(inErr), outErr)
		}

		// Ensure that we did not log to the system log
		if sysLogBuf.Len() > 0 {
			t.Fatalf("Unexpected data logged to system log for driver '%s'. Expected 0 bytes written, got %d bytes written: '%s'",
				tc.name, sysLogBuf.Len(), strings.TrimRight(sysLogBuf.String(), "\n"))
		}

		// Ensure that the expected message was logged
		loggedMsg := ""
		if tc.logger != nil || tc.ctxLogger != nil {
			loggedMsg = strings.TrimRight(captureBuf.String(), "\n")
		}

		if tc.expectedMsg != loggedMsg {
			t.Fatalf("Unexpected data logged for test case '%s'. Expected '%s', got: '%s'",
				tc.name, tc.expectedMsg, loggedMsg)
		}
	}
}
