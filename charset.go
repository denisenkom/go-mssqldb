package mssql

type charsetMap struct {
	sb [256]rune    // single byte runes, -1 for a double byte character lead byte
	db map[int]rune // double byte runes
}

func collation2charset(col collation) *charsetMap {
	// http://msdn.microsoft.com/en-us/library/ms144250.aspx
	switch col.sortId {
	case 31, 32, 33, 34:
		return cp437
	case 40, 41, 42, 44, 49, 55, 56, 57, 58, 59, 60, 61:
		return cp850
	case 51, 52, 53, 54:
		return cp1252
	case 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96:
		return cp1250
	case 104, 105, 106, 107, 108:
		return cp1251
	case 112, 113, 114, 121, 124:
		return cp1253
	case 128, 129, 130:
		return cp1254
	case 136, 137, 138:
		return cp1255
	case 144, 145, 146:
		return cp1256
	case 152, 153, 154, 155, 156, 157, 158, 159, 160:
		return cp1257
	case 183, 184, 185, 186:
		return cp1252
	}
	// http://technet.microsoft.com/en-us/library/aa176553(v=sql.80).aspx
	switch col.getLcid() {
	case 0x001e, 0x041e:
		return cp874
	case 0x0411:
		return cp932
	case 0x0804, 0x1004:
		return cp936
	case 0x0012, 0x0412:
		return cp949
	case 0x0404, 0x1404, 0x0c04, 0x7c04:
		return cp950
	case 0x041c, 0x041a, 0x0405, 0x040e, 0x104e, 0x0415, 0x0418, 0x041b, 0x0424:
		return cp1250
	case 0x0423, 0x0402, 0x042f, 0x0419, 0x081a, 0x0c1a, 0x0422:
		return cp1251
	case 0x0408:
		return cp1253
	case 0x041f, 0x042c, 0x0443:
		return cp1254
	case 0x040d:
		return cp1255
	case 0x0401, 0x0801, 0xc01, 0x1001, 0x1401, 0x1801, 0x1c01, 0x2001, 0x2401, 0x2801, 0x2c01, 0x3001, 0x3401, 0x3801, 0x3c01, 0x4001, 0x0429, 0x0420:
		return cp1256
	case 0x0425, 0x0426, 0x0427, 0x0827:
		return cp1257
	case 0x042a:
		return cp1258
	case 0x0439:
		return nil
	}
	return cp1252
}

func charset2utf8(col collation, s []byte) string {
	cm := collation2charset(col)
	if cm == nil {
		return string(s)
	}
	buf := make([]rune, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := cm.sb[s[i]]
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
