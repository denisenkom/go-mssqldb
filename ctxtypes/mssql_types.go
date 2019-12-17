package ctxtypes

// ContextKey is, as the name implied, a type reserved
// for keys when passing values into the context
type ContextKey string

// PreLoginResponseKey is used to obtain PreLogin Response fields
const PreLoginResponseKey ContextKey = "preLoginResponse"
// ClientLoginKey is used to pass the client Login
const ClientLoginKey ContextKey = "clientLogin"
// ServerLoginAckKey is used to pass the server LoginAck
const ServerLoginAckKey ContextKey = "serverLoginAck"
// ServerErrorKey is used to pass the server error
const ServerErrorKey ContextKey = "serverError"
