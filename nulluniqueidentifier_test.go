package mssql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
)

func TestNullUniqueIdentifier(t *testing.T) {
	dbUUID := []byte{0x67, 0x45, 0x23, 0x01,
		0xAB, 0x89,
		0xEF, 0xCD,
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
	}

	uuid := NullUniqueIdentifier{Valid: true, UUID: [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}}

	t.Run("Scan", func(t *testing.T) {
		t.Run("[]byte", func(t *testing.T) {
			var sut NullUniqueIdentifier
			if err := sut.Scan(dbUUID[:]); err != nil {
				t.Fatal(err)
			}
			if sut != uuid {
				t.Errorf("bytes not swapped correctly: got %q; want %q", sut, uuid)
			}
		})

		t.Run("string", func(t *testing.T) {
			var sut NullUniqueIdentifier
			if err := sut.Scan(uuid.String()); err != nil {
				t.Fatal(err)
			}
			if sut != uuid {
				t.Errorf("string not scanned correctly: got %q; want %q", sut, uuid)
			}
		})
	})

	t.Run("Value", func(t *testing.T) {
		sut := uuid
		v, err := sut.Value()
		if err != nil {
			t.Fatal(err)
		}

		b, ok := v.([]byte)
		if !ok {
			t.Fatalf("(%T) is not []byte", v)
		}

		if !bytes.Equal(b, dbUUID[:]) {
			t.Errorf("got %q; want %q", b, dbUUID)
		}
	})
}

func TestNullUniqueIdentifierString(t *testing.T) {
	sut := NullUniqueIdentifier{Valid: true, UUID: [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}}
	expected := "01234567-89AB-CDEF-0123-456789ABCDEF"
	if actual := sut.String(); actual != expected {
		t.Errorf("sut.String() = %s; want %s", sut, expected)
	}
}

func TestNullUniqueIdentifierMarshalText(t *testing.T) {
	sut := NullUniqueIdentifier{Valid: true, UUID: [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}}
	expected := []byte{48, 49, 50, 51, 52, 53, 54, 55, 45, 56, 57, 65, 66, 45, 67, 68, 69, 70, 45, 48, 49, 50, 51, 45, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70}
	if actual := sut.MarshalText(); !reflect.DeepEqual(actual, expected) {
		t.Errorf("sut.MarshalText() = %v; want %v", actual, expected)
	}
}

var _ fmt.Stringer = NullUniqueIdentifier{}
var _ sql.Scanner = &NullUniqueIdentifier{}
var _ driver.Valuer = NullUniqueIdentifier{}
