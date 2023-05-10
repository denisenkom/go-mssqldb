package mssql

import "testing"

func TestNullUniqueIdentifier(t *testing.T) {
	dbUUID := UniqueIdentifier{0x67, 0x45, 0x23, 0x01,
		0xAB, 0x89,
		0xEF, 0xCD,
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
	}

	uuid := UniqueIdentifier{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}

	t.Run("Scan", func(t *testing.T) {

		t.Run("[]byte", func(t *testing.T) {

			var nui NullUniqueIdentifier
			if err := nui.Scan(dbUUID[:]); err != nil {

				t.Fatal(err)
			}
			if nui.UniqueIdentifier != uuid {

				t.Errorf("bytes not swapped correctly: got %q; want %q", nui.UniqueIdentifier, uuid)
			}
		})

		t.Run("string", func(t *testing.T) {

			var nui NullUniqueIdentifier
			if err := nui.Scan(uuid.String()); err != nil {

				t.Fatal(err)
			}
			if nui.UniqueIdentifier != uuid {

				t.Errorf("bytes not swapped correctly: got %q; want %q", nui.UniqueIdentifier, uuid)
			}
		})

		t.Run("nil", func(t *testing.T) {

			var nui NullUniqueIdentifier
			var null interface{}
			if err := nui.Scan(null); err != nil {

				t.Fatal(err)
			}
			if nui.Valid {

				t.Errorf("Validity not correct: got %t; want false", nui.Valid)
			}
		})
	})

	t.Run("String", func(t *testing.T) {

		t.Run("Empty string", func(t *testing.T) {

			var nui NullUniqueIdentifier
			var null interface{}
			if err := nui.Scan(null); err != nil {

				t.Fatal(err)
			}

			if str := nui.String(); str != "" {

				t.Errorf("String invalid: got %s; want %s", str, `""`)
			}
		})

		t.Run("String", func(t *testing.T) {

			var nui NullUniqueIdentifier
			if err := nui.Scan(dbUUID[:]); err != nil {

				t.Fatal(err)
			}

			if str := nui.String(); str == "" {

				t.Errorf("String invalid: got %s; want %s", "67452301-AB89-EFCD-0123-456789ABCDEF", str)
			}
		})
	})
}
