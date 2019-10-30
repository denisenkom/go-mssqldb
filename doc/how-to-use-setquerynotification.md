# How create a Query Notification

Query notifications subscriptions can be set on queries to request that the application be notified when the results of the query change. After using the driver's `Conn` type to `Prepare` a query, call `SetQueryNotification()` method to set the contents of the query notification header.

```go
    sqlstmt.SetQueryNotification("Message", "service=myService", time.Hour)
```

The parameters of the `SetQueryNotification()` method are: `message`, `options`, and `timeout`. The `options` parameter takes a string containing the service name as well as the database or broker instance. `options` must be in the following format:

`service=<service-name>[;(local database=<database> | broker instance=<broker instance>)]`

The time unit for the `timeout` parameter is milliseconds.
The query for notification must be in the correct [format](https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175110(v=sql.105)) or the subscription will fail on the server.

## Example

[Query Notification Example](..\setquerynotification_example_test.go)

## Useful Links

- [Using Query Notifications](https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175110(v=sql.105))
- [Working with Query Notifications](https://docs.microsoft.com/en-us/sql/relational-databases/native-client/features/working-with-query-notifications?view=sql-server-2017)
- [Creating a Query for Notification](https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms181122(v=sql.105))
- [Query Notifications Header](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/e168d373-a7b7-41aa-b6ca-25985466a7e0)