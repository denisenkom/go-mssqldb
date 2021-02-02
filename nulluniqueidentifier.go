package mssql

// NullUniqueIdentifier represents a GUID that may be null.
// NullUniqueIdentifier implements the Scanner interface so it can be used as a scan destination.
type NullUniqueIdentifier struct {
	UniqueIdentifier UniqueIdentifier
	Valid            bool
}

// Scan implements the Scanner interface.
func (nui *NullUniqueIdentifier) Scan(v interface{}) error {

	if v == nil {

		nui.Valid = false
		return nil
	}

	err := nui.UniqueIdentifier.Scan(v)
	if err != nil {

		nui.Valid = false
		return nil
	}

	nui.Valid = true
	return nil
}

// String returns the UniqueIdentifier value
func (nui NullUniqueIdentifier) String() string {

	if !nui.Valid {

		return ""
	}

	return nui.UniqueIdentifier.String()
}
