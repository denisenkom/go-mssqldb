package mssql

import "context"

// contextKey is, as the name implied, a type reserved
// for keys when passing values into the context
type contextKey string
const connectInterceptorKey contextKey = "connectInterceptor"

// ConnectInterceptor is used to exchange values between the driver and the user during
// the connection phase
type ConnectInterceptor struct {
	// ServerPreLoginResponse is used to obtain PreLogin Response fields
	ServerPreLoginResponse chan map[uint8][]byte
	// ClientLoginRequest is used to pass the client LoginRequest
	ClientLoginRequest chan *LoginRequest
}

// NewConnectInterceptor is a constructor for a blank ConnectInterceptor
func NewConnectInterceptor() *ConnectInterceptor {
	return &ConnectInterceptor{
		// Create a channel for receiving the prelogin response through the context
		ServerPreLoginResponse: make(chan map[uint8][]byte),
		// Create a channel for sending the client login to the driver through the context
		ClientLoginRequest: make(chan *LoginRequest),
	}
}

// NewContextWithConnectInterceptor returns a new Context that carries value ci.
func NewContextWithConnectInterceptor(ctx context.Context, u *ConnectInterceptor) context.Context {
	return context.WithValue(ctx, connectInterceptorKey, u)
}

// ConnectInterceptorFromContext returns the ConnectInterceptor value stored in ctx, if any.
func ConnectInterceptorFromContext(ctx context.Context) *ConnectInterceptor {
	ci := ctx.Value(connectInterceptorKey)
	if ci == nil {
		return nil
	}

	return ci.(*ConnectInterceptor)
}
