# Overview

.NET Standart Data Provider for MonetDb

https://www.nuget.org/packages/MonetDb.Mapi/

Tests included.

Supported Mapi protocols:
 - version 8
 - version 9 

# Versions
 - 2.1.0 Cancelling command by terminate session/connection. For this feature use `MonetDbEnviroments.CommandCloseStrategy`
 - 2.0.1 HugeInt DataType support
 - 2.0.0 Redirect connection, Web Api Client (.NET Core 2.2 + ReactJs).
 - 1.6.0 MonetDbConnectionStringBuilder
 - 1.5.0 Need more support, copy from stdin test
 - 1.4.1 `BugFix` error handler
 - 1.4.0 Large query support
  - 1.3.6 Default Isolation Level Unspecified -> Serializable
  - 1.3.5 `BugFix` correct char dbtype
  - 1.3.4 Date parser, tests
  - 1.3.3 Data/Db types
  - 1.3.2 `BugFix` multiline exceptions, disposing connections, parallelling pool
  - 1.3.1 `BugFix` "Unrecognized {number}]"
 - 1.3 New lexer for parsing mapi data rows
 - 1.2 Change implementig System.Data interfaces to extending abstract classes: System.Data.IDb... -> System.Data.Common.Db...
 - 1.1.1 double E format parsing

# Web Client
### Connection
![Connection](https://raw.githubusercontent.com/AlexandrSitdikov/monetdb-mapi-net/master/content/desc_connection.png "Connection")
### Query
![Query](https://raw.githubusercontent.com/AlexandrSitdikov/monetdb-mapi-net/master/content/desc_query.png "Query")
### Result
![Result](https://raw.githubusercontent.com/AlexandrSitdikov/monetdb-mapi-net/master/content/desc_result.png "Result")
### Mobile result
![Mobile](https://raw.githubusercontent.com/AlexandrSitdikov/monetdb-mapi-net/master/content/phone_result.png "Mobile")
