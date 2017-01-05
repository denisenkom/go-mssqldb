package mssql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
)

func TestUniqueIdentifier(t *testing.T) {
	dbUUID := []byte{0x67, 0x45, 0x23, 0x01,
		0xAB, 0x89,
		0xEF, 0xCD,
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
	}

	uuid := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}

	t.Run("Scan", func(t *testing.T) {
		t.Run("[]byte", func(t *testing.T) {
			sut := new(UniqueIdentifier)
			if err := sut.Scan(dbUUID); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal([]byte(*sut), uuid) {
				t.Errorf("bytes not swapped correctly: got %q; want %q", []byte(*sut), uuid)
			}
		})

		t.Run("string", func(t *testing.T) {
			sut := new(UniqueIdentifier)
			if err := sut.Scan(UniqueIdentifier(uuid).String()); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal([]byte(*sut), uuid) {
				t.Errorf("string not scanned correctly: got %q; want %q", *sut, uuid)
			}
		})
	})

	t.Run("Value", func(t *testing.T) {
		sut := UniqueIdentifier(uuid)
		v, err := sut.Value()
		if err != nil {
			t.Fatal(err)
		}

		b, ok := v.([]byte)
		if !ok {
			t.Fatalf("(%T) is not []byte", v)
		}

		if !bytes.Equal(b, dbUUID) {
			t.Errorf("got %q; want %q", b, dbUUID)
		}
	})

	t.Run("Equal", func(t *testing.T) {
		sut := make(UniqueIdentifier, len(uuid))
		uuid2 := make(UniqueIdentifier, len(uuid))

		copy(sut, uuid)
		copy(uuid2, uuid)

		if actual := sut.Equal(uuid2); actual != true {
			t.Errorf("sut.Equal(uuid2) = %t; want %t", actual, true)
		}
	})

	t.Run("NotEqual", func(t *testing.T) {
		sut := make(UniqueIdentifier, len(uuid))
		uuid2 := make(UniqueIdentifier, len(uuid))

		copy(sut, uuid)
		copy(uuid2, dbUUID)

		if actual := sut.Equal(uuid2); actual != false {
			t.Errorf("sut.Equal(uuid2) = %t; want %t", actual, true)
		}
	})
}

func TestUniqueIdentifierString(t *testing.T) {
	sut := UniqueIdentifier{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}
	expected := "01234567-89ab-cdef-0123-456789abcdef"
	if actual := sut.String(); actual != expected {
		t.Errorf("sut.String() = %s; want %s", sut, expected)
	}
}

func TestUniqueIdentifierImplementsStringer(t *testing.T) {
	var v interface{}
	v = new(UniqueIdentifier)

	if _, ok := v.(fmt.Stringer); !ok {
		t.Error(`Uniqueidentifier must be fmt.Stringer`)
	}

}
func TestUniqueIdentifierImplementsScanner(t *testing.T) {
	var v interface{}
	v = new(UniqueIdentifier)

	if _, ok := v.(sql.Scanner); !ok {
		t.Error(`Uniqueidentifier must be "database/sql".Scanner`)
	}
}

func TestUniqueIdentifierImplementsValuer(t *testing.T) {
	var v interface{}
	v = new(UniqueIdentifier)

	if _, ok := v.(driver.Valuer); !ok {
		t.Error(`Uniqueidentifier must be "database/sql/driver".Valuer`)
	}
}
