package mssql

type cekTable struct {
	entries []cekTableEntry
}

type encryptionKeyInfo struct {
	encryptedKey  []byte
	databaseID    int
	cekID         int
	cekVersion    int
	cekMdVersion  []byte
	keyPath       string
	keyStoreName  string
	algorithmName string
}

type cekTableEntry struct {
	databaseID int
	keyId      int
	keyVersion int
	mdVersion  []byte
	valueCount int
	cekValues  []encryptionKeyInfo
}

func newCekTable(size uint16) cekTable {
	return cekTable{entries: make([]cekTableEntry, size)}
}