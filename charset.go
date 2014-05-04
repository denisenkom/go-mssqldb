package mssql

type charsetMap struct {
	sb [256]rune
	db map[int]rune
}

func collation2charset(col collation) *charsetMap {
	if col.getLcid() == 0x0408 {
		return cp1253
	}
	return nil
}

func charset2utf8(col collation, s []byte) string {
	cm := collation2charset(col)
	if cm == nil {
		return string(s)
	}
	buf := make([]rune, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := cm.sb[s[i]]
		// -1 is the double byte marker
		if ch == -1 {
			if i+1 == len(s) {
				ch = 0xfffd
			} else {
				n := int(s[i]) + (int(s[i+1]) << 8)
				i++
				var ok bool
				ch, ok = cm.db[n]
				if !ok {
					ch = 0xfffd
				}
			}
		}
		buf = append(buf, ch)
	}
	return string(buf)
}
