# How to use Table-Valued Parameters

Table-valued parameters are declared by using user-defined table types. You can use table-valued parameters to send multiple rows of data to a Transact-SQL statement or a routine, such as a stored procedure or function, without creating a temporary table or many parameters.

To make use of the TVP functionality, first you need to create a table type, and a procedure or function to receive data from the table-valued parameter.

```
createTVP = "CREATE TYPE LocationTableType AS TABLE (LocationName VARCHAR(50), CostRate INT)"
_, err = db.Exec(createTable)

createProc = `
CREATE PROCEDURE dbo.usp_InsertProductionLocation
@TVP LocationTableType READONLY
AS
SET NOCOUNT ON
INSERT INTO Location
(
	Name,
	CostRate,
	Availability,
	ModifiedDate)
SELECT *, 0,GETDATE()
FROM @TVP`
_, err = db.Exec(createProc)
```

In your go application, create a struct that corresponds to the table type you have created. Create a slice of these structs which contain the data you want to pass to the stored procedure.

```
type LocationTableTvp struct {
	LocationName string
	CostRate     int64
}

locationTableTypeData := []LocationTableTvp{
	{
		LocationName: "Alberta",
		CostRate:     0,
	},
	{
		LocationName: "British Columbia",
		CostRate:     1,
	},
}
```

Create a `mssql.TVPType` object, and pass the slice of structs into the `TVPValue` member. Set `TVPTypeName` to the table type name, and `TVPScheme` to the schema which the table type belongs to. If `TVPScheme` is not specified, SQL Server searches the table type from the default schema of your user in SQL Server. Note the `TVPTypeName` and `TVPScheme` members may be combined in future implementations of the go-mssqldb driver (see [issue 458](https://github.com/denisenkom/go-mssqldb/issues/458)).

```
tvpType := mssql.TVPType{
	TVPTypeName: "LocationTableType",
	TVPScheme:   "",
	TVPValue:    locationTableTypeData,
}
```

Finally, execute the stored procedure and pass the `mssql.TVPType` object you have created as a parameter.

`_, err = db.Exec("exec dbo.usp_InsertProductionLocation @TVP;", sql.Named("TVP", tvpType))`

## Example
[TVPType example](../tvptype_example_test.go)
