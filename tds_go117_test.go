//go:build go1.17
// +build go1.17

package mssql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math/big"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
	"github.com/stretchr/testify/assert"
)

func TestChangePassword(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()
	login, pwd := createLogin(t, conn)
	defer dropLogin(t, conn, login)
	p, err := msdsn.Parse(makeConnStr(t).String())
	assert.NoError(t, err, "Parse failed")
	p.ChangePassword = "Change" + pwd
	p.User = login
	p.Password = pwd
	p.Parameters[msdsn.UserID] = p.User
	p.Parameters[msdsn.Password] = p.Password
	tl := testLogger{t: t}
	defer tl.StopLogging()
	c, err := connect(context.Background(), &Connector{params: p}, optionalLogger{loggerAdapter{&tl}}, p)
	if assert.NoError(t, err, "Login with new login failed") {
		c.buf.transport.Close()

		p.Password = p.ChangePassword
		p.ChangePassword = ""
		c, err = connect(context.Background(), &Connector{params: p}, optionalLogger{loggerAdapter{&tl}}, p)
		if assert.NoError(t, err, "Login with new password failed") {
			c.buf.transport.Close()
		}
	}

}

func createLogin(t *testing.T, conn *sql.DB) (login string, password string) {
	t.Helper()
	suffix, _ := rand.Int(rand.Reader, big.NewInt(10000))
	login = fmt.Sprintf("mssqlLogin%d", suffix.Int64())
	password = fmt.Sprintf("mssqlPwd!%d", suffix.Int64())
	_, err := conn.Exec(fmt.Sprintf("CREATE LOGIN [%s] WITH PASSWORD = '%s', CHECK_POLICY=OFF\nCREATE USER %s", login, password, login))
	assert.NoError(t, err, "create login failed")
	return
}

func dropLogin(t *testing.T, conn *sql.DB, login string) {
	t.Helper()
	_, _ = conn.Exec(fmt.Sprintf("DROP USER %s\nDROP LOGIN [%s]", login, login))
}
