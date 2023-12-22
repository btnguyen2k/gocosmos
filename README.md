# gocosmos

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/gocosmos)](https://goreportcard.com/report/github.com/btnguyen2k/gocosmos)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/gocosmos)](https://pkg.go.dev/github.com/btnguyen2k/gocosmos)
[![Actions Status](https://github.com/btnguyen2k/gocosmos/workflows/gocosmos/badge.svg)](https://github.com/btnguyen2k/gocosmos/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/gocosmos/branch/main/graph/badge.svg)](https://codecov.io/gh/btnguyen2k/gocosmos)
[![Release](https://img.shields.io/github/release/btnguyen2k/gocosmos.svg?style=flat-square)](RELEASE-NOTES.md)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database-drivers)

Go driver for [Azure Cosmos DB SQL API](https://azure.microsoft.com/services/cosmos-db/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package. A REST client is also included.

## database/sql driver

Summary of supported SQL statements:

| Statement                                   | Syntax                                                                                   |
|---------------------------------------------|------------------------------------------------------------------------------------------|
| Create a new database                       | `CREATE DATABASE [IF NOT EXISTS] <db-name>`                                              |
| Change database's throughput                | `ALTER DATABASE <db-name> WITH RU/MAXRU=<ru>`                                            |
| Delete an existing database                 | `DROP DATABASE [IF EXISTS] <db-name>`                                                    |
| List all existing databases                 | `LIST DATABASES`                                                                         |
| Create a new collection                     | `CREATE COLLECTION [IF NOT EXISTS] [<db-name>.]<collection-name> <WITH PK=partitionKey>` |
| Change collection's throughput              | `ALTER COLLECTION [<db-name>.]<collection-name> WITH RU/MAXRU=<ru>`                      |
| Delete an existing collection               | `DROP COLLECTION [IF EXISTS] [<db-name>.]<collection-name>`                              |
| List all existing collections in a database | `LIST COLLECTIONS [FROM <db-name>]`                                                      |
| Insert a new document into collection       | `INSERT INTO [<db-name>.]<collection-name> ...`                                          |
| Insert or replace a document                | `UPSERT INTO [<db-name>.]<collection-name> ...`                                          |
| Delete an existing document                 | `DELETE FROM [<db-name>.]<collection-name> WHERE id=<id-value>`                          |
| Update an existing document                 | `UPDATE [<db-name>.]<collection-name> SET ... WHERE id=<id-value>`                       |
| Query documents in a collection             | `SELECT [CROSS PARTITION] ... FROM <collection-name> ... [WITH database=<db-name>]`      |

See [supported SQL statements](SQL.md) for details.

> Azure Cosmos DB SQL API currently supports only [SELECT statement](https://learn.microsoft.com/azure/cosmos-db/nosql/query/select).
> `gocosmos` implements other statements by translating the SQL statement to [REST API calls](https://learn.microsoft.com/rest/api/cosmos-db/).

### Example usage:

```go
package main

import (
	"database/sql"
	_ "github.com/btnguyen2k/gocosmos"
)

func main() {
	driver := "gocosmos"
	dsn := "AccountEndpoint=https://localhost:8081/;AccountKey=<cosmosdb-account-key>"
	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	
	_, err = db.Exec("CREATE DATABASE mydb WITH maxru=10000")
	if err != nil {
		panic(err)
	}
	
	// database "mydb" has been created successfuly
}
```

**Data Source Name (DSN) syntax for Cosmos DB**

_Note: line-break is for readability only!_

```connection
AccountEndpoint=<cosmosdb-endpoint>
;AccountKey=<cosmosdb-account-key>
[;TimeoutMs=<timeout-in-ms>]
[;Version=<cosmosdb-api-version>]
[;DefaultDb|Db=<db-name>]
[;AutoId=<true/false>]
[;InsecureSkipVerify=<true/false>]
```

- `AccountEndpoint`: (required) endpoint to access Cosmos DB. For example, the endpoint for Azure Cosmos DB Emulator running on local is `https://localhost:8081/`.
- `AccountKey`: (required) account key to authenticate.
- `TimeoutMs`: (optional) operation timeout in milliseconds. Default value is `10 seconds` if not specified.
- `Version`: (optional) version of Cosmos DB to use. Default value is `2020-07-15` if not specified. See: https://learn.microsoft.com/rest/api/cosmos-db/#supported-rest-api-versions.
- `DefaultDb`: (optional, available since [v0.1.1](RELEASE-NOTES.md)) specify the default database used in Cosmos DB operations. Alias `Db` can also be used instead of `DefaultDb`.
- `AutoId`: (optional, available since [v0.1.2](RELEASE-NOTES.md)) see [auto id](#auto-id) session.
- `InsecureSkipVerify`: (optional, available since [v0.1.4](RELEASE-NOTES.md)) if `true`, disable CA verification for https endpoint (useful to run against test/dev env with local/docker Cosmos DB emulator).

### Auto-id

Azure Cosmos DB requires each document has a [unique ID](https://learn.microsoft.com/rest/api/cosmos-db/documents) that identifies the document.
When creating new document, if value for the unique ID field is not supplied `gocosmos` is able to generate one automatically. This feature is enabled
by specifying setting `AutoId=true` in the Data Source Name (for `database/sql` driver) or the connection string (for REST client). If not specified, default
value is `AutoId=true`.

_This setting is available since [v0.1.2](RELEASE-NOTES.md)._

### Known issues

**`GROUP BY` combined with `ORDER BY` is not supported**

Azure Cosmos DB does not support `GROUP BY` combined with `ORDER BY` yet. You will receive the following error message:

> 'ORDER BY' is not supported in presence of GROUP BY.

**Cross-partition paging**

Cross-partition paging can be done with the `OFFSET...LIMIT` clause. However, the query is not stable without `ORDER BY`. The returned results may not be consistent from query to query.

**Queries that may consume a large amount of memory**

These queries may consume a large amount of memory if executed against a large table:

- `OFFSET...LIMIT` clause with big offset or limit values.
- `SELECT DISTINCT` and `SELECT DISTINCT VALUE` queries.
- Queries with `GROUP BY` clause.

## REST client

See the [REST.md](REST.md) file for details.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Support and Contribution

Feel free to create [pull requests](https://github.com/btnguyen2k/gocosmos/pulls) or [issues](https://github.com/btnguyen2k/gocosmos/issues) to report bugs or suggest new features.
Please search the existing issues before filing new issues to avoid duplicates. For new issues, file your bug or feature request as a new issue.

If you find this project useful, please start it.
