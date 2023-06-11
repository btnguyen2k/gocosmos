# gocosmos

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/gocosmos)](https://goreportcard.com/report/github.com/btnguyen2k/gocosmos)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/gocosmos)](https://pkg.go.dev/github.com/btnguyen2k/gocosmos)
[![Actions Status](https://github.com/btnguyen2k/gocosmos/workflows/gocosmos/badge.svg)](https://github.com/btnguyen2k/gocosmos/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/gocosmos/branch/main/graph/badge.svg?token=pYdHuxbIiI)](https://codecov.io/gh/btnguyen2k/gocosmos)
[![Release](https://img.shields.io/github/release/btnguyen2k/gocosmos.svg?style=flat-square)](RELEASE-NOTES.md)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database-drivers)

Go driver for [Azure Cosmos DB SQL API](https://azure.microsoft.com/services/cosmos-db/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package. A REST client is also included.

## database/sql driver

Summary of supported SQL statements:

|Statement|Syntax|
|---------|-----------|
|Create a new database                      |`CREATE DATABASE [IF NOT EXISTS] <db-name>`|
|Change database's throughput               |`ALTER DATABASE <db-name> WITH  RU/MAXRU=<ru>`|
|Delete an existing database                |`DROP DATABASE [IF EXISTS] <db-name>`|
|List all existing databases                |`LIST DATABASES`|
|Create a new collection                    |`CREATE COLLECTION [IF NOT EXISTS] [<db-name>.]<collection-name> <WITH [LARGE]PK=partitionKey>`|
|Change collection's throughput             |`ALTER COLLECTION [<db-name>.]<collection-name> WITH  RU/MAXRU=<ru>`|
|Delete an existing collection              |`DROP COLLECTION [IF EXISTS] [<db-name>.]<collection-name>`|
|List all existing collections in a database|`LIST COLLECTIONS [FROM <db-name>]`|
|Insert a new document into collection      |`INSERT INTO [<db-name>.]<collection-name> ...`|
|Insert or replace a document               |`UPSERT INTO [<db-name>.]<collection-name> ...`|
|Delete an existing document                |`DELETE FROM [<db-name>.]<collection-name> WHERE id=<id-value>`|
|Update an existing document                |`UPDATE [<db-name>.]<collection-name> SET ... WHERE id=<id-value>`|
|Query documents in a collection            |`SELECT [CROSS PARTITION] ... FROM <collection-name> ... [WITH database=<db-name>]`|

See [supported SQL statements](SQL.md) for details.

> Azure Cosmos DB SQL API currently supports only [SELECT statement](https://learn.microsoft.com/azure/cosmos-db/nosql/query/select).
> `gocosmos` implements other statements by translating the SQL statement to [REST API calls](https://learn.microsoft.com/rest/api/cosmos-db/).

### Example usage:

```go
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

	_, err := db.Exec("CREATE DATABASE mydb WITH maxru=10000")
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

_This settings is available since [v0.1.2](RELEASE-NOTES.md)._

### Known issues

**`GROUP BY` combined with `ORDER BY` is not supported**

Azure Cosmos DB does not support `GROUP BY` combined with `ORDER BY` yet. You will receive the following error message:

> 'ORDER BY' is not supported in presence of GROUP BY.

**Cross-partition paging**

The `database/sql` driver does not use `max-count-item` like the REST client. Hence the only paging technique that can
be used is `OFFSET...LIMIT`. The underlying driver uses `the `RestClient.QueryDocumentsCrossPartition(...)` to fetch
rows from Cosmos DB. Thus, some limitations:

- `OFFSET...LIMIT` without `ORDER BY`: 

## REST client

The REST client supports:
- Database: `Create`, `Get`, `Delete`, `List` and change throughput.
- Collection: `Create`, `Replace`, `Get`, `Delete`, `List` and change throughput.
- Document: `Create`, `Replace`, `Get`, `Delete`, `Query` and `List`.

### Example usage: 

```go
import (
	"os"
	"database/sql"
	"github.com/btnguyen2k/gocosmos"
)

func main() {
	cosmosDbConnStr := "AccountEndpoint=https://localhost:8081/;AccountKey=<cosmosdb-account-key>"
	client, err := gocosmos.NewRestClient(nil, cosmosDbConnStr)
	if err != nil {
		panic(err)
	}

	dbSpec := gocosmos.DatabaseSpec{Id:"mydb", Ru: 400}
	result := client.CreateDatabase(dbSpec)
	if result.Error() != nil {
		panic(result.Error)
	}

	// database "mydb" has been created successfuly
}
```

**Connection string syntax for Cosmos DB**

_Note: line-break is for readability only!_

```
AccountEndpoint=<cosmosdb-endpoint>
;AccountKey=<cosmosdb-account-key>
[;TimeoutMs=<timeout-in-ms>]
[;Version=<cosmosdb-api-version>]
[;AutoId=<true/false>]
[;InsecureSkipVerify=<true/false>`]
```

- `AccountEndpoint`: (required) endpoint to access Cosmos DB. For example, the endpoint for Azure Cosmos DB Emulator running on local is `https://localhost:8081/`.
- `AccountKey`: (required) account key to authenticate.
- `TimeoutMs`: (optional) operation timeout in milliseconds. Default value is `10 seconds` if not specified.
- `Version`: (optional) version of Cosmos DB to use. Default value is `2020-07-15` if not specified. See: https://learn.microsoft.com/rest/api/cosmos-db/#supported-rest-api-versions.
- `AutoId`: (optional, available since [v0.1.2](RELEASE-NOTES.md)) see [auto id](#auto-id) session.
- `InsecureSkipVerify`: (optional, available since [v0.1.4](RELEASE-NOTES.md)) if `true`, disable CA verification for https endpoint (useful to run against test/dev env with local/docker Cosmos DB emulator).

### Known issues

**`GROUP BY` combined with `ORDER BY` is not supported**

Azure Cosmos DB does not support `GROUP BY` combined with `ORDER BY` yet. You will receive the following error message:

> 'ORDER BY' is not supported in presence of GROUP BY.

**Cross-partition queries**

When documents are spanned across partitions, they must be fetched from multiple `PkRangeId` and then merged to build
the final result. Due to this behaviour, some cross-partition queries might not work as expected.

- *Paging cross-partition `OFFSET...LIMIT` queries using `max-count-item`*:<br>
  Since documents must be fetched from multiple `PkRangeId`, the result is nondeterministic.
  Moreover, calls to `RestClient.QueryDocumentsCrossPartition(...)` and `RestClient.QueryDocuments(...)` without
  pagination (i.e. set `MaxCountItem=0`) may yield different results.

- *Paging cross-partition `ORDER BY` queries with `max-count-item`*:<br>
  Due to the fact that documents must be fetched from multiple `PkRangeId`, rows returned from calls to
  `RestClient.QueryDocuments(...)` might not be in the expected order.<br>
  *Workaround*: if you can afford the memory, use `RestClient.QueryDocumentsCrossPartition(...)` or
  `RestClient.QueryDocuments(...)` without pagination (i.e. set `MaxCountItem=0`).

- *Paging `SELECT DISTINCT` queries with `max-count-item`*:<br>
  Due to the fact that documents must be fetched from multiple `PkRangeId`, rows returned from calls to
  `RestClient.QueryDocuments(...)` might be duplicated.<br>
  *Workaround*: if you can afford the memory, use `RestClient.QueryDocumentsCrossPartition(...)` or
  `RestClient.QueryDocuments(...)` without pagination (i.e. set `MaxCountItem=0`).
  
- *`GROUP BY` combined with `max-count-item`*:<br>
  Due to the fact that documents must be fetched from multiple `PkRangeId`, the result returned from calls to
  `RestClient.QueryDocuments(...)` might not be as espected.<br>
  *Workaround*: if you can afford the memory, use `RestClient.QueryDocumentsCrossPartition(...)` or
  `RestClient.QueryDocuments(...)` without pagination (i.e. set `MaxCountItem=0`).

## License

MIT - see [LICENSE.md](LICENSE.md).
