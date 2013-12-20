package mssql


type Error struct {
    Number int32
    State uint8
    Class uint8
    Message string
    ServerName string
    ProcName string
    LineNo int32
}


func (e Error) Error() string {
    return "mssql: " + e.Message
}
