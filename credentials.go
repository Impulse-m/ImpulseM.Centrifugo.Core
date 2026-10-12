package centrifuge

import "context"

// Credentials allow authenticating connection when set into context.
type Credentials struct {
	// UserID tells library an ID of current user. Leave this empty string
	// if you need access from anonymous user.
	UserID string
	// ExpireAt allows setting time in future when connection must be validated.
	// In this case Client.OnRefresh callback must be set by application. Zero
	// value means no expiration.
	ExpireAt int64
	// Info contains additional information about connection. This data will be
	// included untouched into Join/Leave messages, into Presence information,
	// also info can become a part of published message as part of ClientInfo.
	// In some cases having additional info can be an undesired overhead – but
	// you are simply free to not use this field at all.
	Info []byte
	// TokenIdentity identifies the token connection was authenticated with. It is
	// available through Client.TokenIdentity and is not sent to the client.
	TokenIdentity TokenIdentity
}

// TokenIdentity identifies the token a connection was authenticated with. It is
// set from Credentials at connect and can be replaced on refresh with
// RefreshReply.TokenIdentity. Application uses it for token revocation checks.
type TokenIdentity struct {
	// Session is an identifier of the session the token was issued for.
	Session [16]byte
	// Generation is a revocation generation of the token.
	Generation uint64
	// IssuedAt is a Unix time in seconds when the token was issued.
	IssuedAt int64
}

// credentialsContextKeyType is special type to safely use context for setting
// and getting Credentials.
type credentialsContextKeyType int

// credentialsContextKey allows Go code to set Credentials into context.
var credentialsContextKey credentialsContextKeyType

// SetCredentials allows setting connection Credentials to Context. Credentials set
// to Context in authentication middleware will be used by Centrifuge library to
// authenticate user.
func SetCredentials(ctx context.Context, cred *Credentials) context.Context {
	ctx = context.WithValue(ctx, credentialsContextKey, cred)
	return ctx
}

// GetCredentials allows extracting Credentials from Context (if set previously).
func GetCredentials(ctx context.Context) (*Credentials, bool) {
	if val := ctx.Value(credentialsContextKey); val != nil {
		cred, ok := val.(*Credentials)
		return cred, ok
	}
	return nil, false
}
