package mssql

import (
	"context"

	"github.com/denisenkom/go-mssqldb/msdsn"
)

const (
	logErrors      = uint64(msdsn.LogErrors)
	logMessages    = uint64(msdsn.LogMessages)
	logRows        = uint64(msdsn.LogRows)
	logSQL         = uint64(msdsn.LogSQL)
	logParams      = uint64(msdsn.LogParams)
	logTransaction = uint64(msdsn.LogTransaction)
	logDebug       = uint64(msdsn.LogDebug)
)

type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type ContextLogger interface {
	Log(ctx context.Context, category msdsn.Log, msg string)
}

// optionalCtxLogger implements the ContextLogger interface with
// a default "do nothing" behavior that can be overridden by an
// optional ContextLogger supplied by the user.
type optionalCtxLogger struct {
	ctxLogger ContextLogger
}

// Log does nothing unless the user has specified an optional
// ContextLogger to override the "do nothing" default behavior.
func (o optionalCtxLogger) Log(ctx context.Context, category msdsn.Log, msg string) {
	if nil != o.ctxLogger {
		o.ctxLogger.Log(ctx, category, msg)
	}
}

type loggerAdapter struct {
	logger Logger
}

func (la loggerAdapter) Log(_ context.Context, _ msdsn.Log, msg string) {
	la.logger.Println(msg)
}
