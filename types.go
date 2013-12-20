package mssql

import (
    "io"
)


type typeInfoIface struct {}

type typeParser func(typeid uint8, r io.Reader) (typeInfoIface, error)
