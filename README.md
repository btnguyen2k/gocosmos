# gocosmos

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/gocosmos)](https://goreportcard.com/report/github.com/btnguyen2k/gocosmos)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/gocosmos)](https://pkg.go.dev/github.com/btnguyen2k/gocosmos)
[![Actions Status](https://github.com/btnguyen2k/gocosmos/workflows/gocosmos/badge.svg)](https://github.com/btnguyen2k/gocosmos/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/gocosmos/branch/main/graph/badge.svg?token=pYdHuxbIiI)](https://codecov.io/gh/btnguyen2k/gocosmos)

Go driver for [Azure Cosmos DB SQL API](https://azure.microsoft.com/en-us/services/cosmos-db/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package.
gocosmos also includes a REST client for [Azure Cosmos DB SQL API](https://azure.microsoft.com/en-us/services/cosmos-db/).

Latest release [v0.1.0](RELEASE-NOTES.md).

## Example usage: REST client

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

## Example usage: database/sql driver

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

## Features

The REST client supports:
- Database: `Create`, `Get`, `Delete` and `List`.
- Collection: `Create`, `Replace`, `Get`, `Delete` and `List`.
- Document: `Create`, `Replace`, `Get`, `Delete`, `Query` and `List`.

The `database/sql` driver supports:
- Database:
  - `CREATE DATABASE`
  - `DROP DATABASE`
  - `LIST DATABASES`
- Table/Collection:
  - `CREATE TABLE/COLLECTION`
  - `DROP TABLE/COLLECTION`
  - `LIST TABLES/COLLECTIONS`
- Item/Document:
  - `INSERT`
  - `UPSERT`
  - `SELECT`
  - `UPDATE`
  - `DELETE`

Summary of supported SQL statements:

|Statement|Syntax|
|---------|-----------|
|Create a new database                      |`CREATE DATABASE [IF NOT EXISTS] <db-name>`|
|Delete an existing database                |`DROP DATABASE [IF EXISTS] <db-name>`|
|List all existing databases                |`LIST DATABASES`|
|Create a new collection                    |`CREATE COLLECTION [IF NOT EXISTS] <db-name>.<collection-name> <WITH [LARGE]PK=partitionKey>`|
|Delete an existing collection              |`DROP COLLECTION [IF EXISTS] <db-name>.<collection-name>`|
|List all existing collections in a database|`LIST COLLECTIONS FROM <db-name>`|
|Insert a new document into collection      |`INSERT INTO <db-name>.<collection-name> ...`|
|Insert or replace a document               |`UPSERT INTO <db-name>.<collection-name> ...`|
|Delete an existing document                |`DELETE FROM <db-name>.<collection-name> WHERE id=<id-value>`|
|Update an existing document                |`UPDATE <db-name>.<collection-name> SET ... WHERE id=<id-value>`|
|Query documents in a collection            |`SELECT [CROSS PARTITION] ... FROM <collection-name> ... WITH database=<db-name>`|

See [supported SQL statements](SQL.md) for details.

## License

MIT - see [LICENSE.md](LICENSE.md).
