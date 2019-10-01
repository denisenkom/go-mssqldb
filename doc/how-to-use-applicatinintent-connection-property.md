# How to use the ApplicationIntent Connection Property

In an Always On Availability Group, support for read-only routing in SQL Server can be configured. Read-only routing refers to the ability of SQL Server to route read-only connection requests to an available Always On secondary replica that is configured to allow read-only workloads when running under the secondary role. To support read-only routing, the availability group must possess an availability group listener. For more information on configuring read-only routing, see [Configure read-only routing for an Always on availability troup](https://docs.microsoft.com/en-us/sql/database-engine/availability-groups/windows/configure-read-only-routing-for-an-availability-group-sql-server?view=sql-server-2017).

For an ease of understanding, let's assume you have the following set up:
- An availability group with primary replica `SQL1` and one secondary replica `SQL2`
- An availability database `CUSTOMER` is added to the availability group
- An availability group listener `AGListener:16333` is added to the availability group
- Read-only routing is configured. The `READ_ONLY_ROUTING_LIST` for `SQL1` is `'SQL2','SQL1'` and the `READ_ONLY_ROUTING_LIST` for `SQL2` is `'SQL1','SQL2'`

An availability group listener is a virtual network name (VNN) to which clients can connect to in order to access database in a primary or secondary replica in an Always On availability group. In this case, the availability group listener `AGListener:16333` can be used as the server name in the connection string. If your intent of the connection is only to read from the database, you can specify the connection property `ApplicationIntent=ReadOnly`. The availability group listener will direct you to the secondary database `SQL2`. If your intent to to write to the database as well, then do not specify the `ApplicationIntent`, and the availability group listener will direct you to the primary database `SQL1`. Furthermore, since replication in an availability group is configured at the level of the database, when the connection property `ApplicationIntent=ReadOnly` is specified, the `database` must also be specified, otherwise connection fails.

Connection string that fails when using `ApplicationIntent=Readonly`:
```
connString := "sqlserver://username:password@AGListener:16333?ApplicationIntent=ReadOnly"
```

Connection string that directs to the secondary replica `SQL2`:
```
connString := "sqlserver://username:password@AGListener:16333?database=CUSTOMER&ApplicationIntent=ReadOnly"
```

Connection string that directs to the primary replica `SQL1`:
```
connString := "sqlserver://username:password@AGListener:16333?database=CUSTOMER"
```
