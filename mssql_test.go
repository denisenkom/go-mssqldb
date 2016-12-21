package mssql

import "testing"

func TestBadOpen(t *testing.T) {
	drv := &MssqlDriver{}
	_, err := drv.open("port=bad")
	if err == nil {
		t.Fail()
	}
}
