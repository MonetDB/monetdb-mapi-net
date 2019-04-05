# Overview

.NET Standart Data Provider for MonetDb

https://www.nuget.org/packages/MonetDb.Mapi/

Tests included.

Supported Mapi protocols:
 - version 8
 - version 9 

# Versions
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
