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

	nui.Valid = true
	return nui.UniqueIdentifier.Scan(v)
}
