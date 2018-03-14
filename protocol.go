package mssql

type Protocol string

const (
	TCP           Protocol = "tcp:"
	NAMED_PIPE    Protocol = "np:"
	SHARED_MEMORY Protocol = "lpc:"
)
