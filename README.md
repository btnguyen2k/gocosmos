# gocosmos

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/gocosmos)](https://goreportcard.com/report/github.com/btnguyen2k/gocosmos)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/gocosmos)](https://pkg.go.dev/github.com/btnguyen2k/gocosmos)
[![Actions Status](https://github.com/btnguyen2k/gocosmos/workflows/gocosmos/badge.svg)](https://github.com/btnguyen2k/gocosmos/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/gocosmos/branch/main/graph/badge.svg?token=pYdHuxbIiI)](https://codecov.io/gh/btnguyen2k/gocosmos)

Go driver for [Azure Cosmos DB SQL API](https://azure.microsoft.com/en-us/services/cosmos-db/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package.
gocosmos also includes a REST client for [Azure Cosmos DB SQL API](https://azure.microsoft.com/en-us/services/cosmos-db/).

Latest release [v0.1.0](RELEASE-NOTES.md).

## Example usage

```go
import (
  "os"
  "database/sql"
  _ "github.com/btnguyen2k/gocosmos"
)

func main() {
  driver := "gocosmos"
	dsn := strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, "")
	db, err := sql.Open(driver, dsn)
	if err != nil {
    panic(err)
  }
  defer db.Close()

	_, err := db.Exec("CREATE DATABASE dbtemp WITH ru=400")
	if err != nil {
    panic(err)
	}
}
```

## Supported statements

- Database:
  - [x] CREATE DATABASE
  - [x] DROP DATABASE
  - [x] LIST DATABASES
- Table/Collection
  - [x] CREATE TABLE/COLLECTION
  - [x] DROP TABLE/COLLECTION
  - [x] LIST TABLES/COLLECTIONS
- Item/Document:
  - [x] INSERT
  - [x] UPSERT
  - [x] SELECT
  - [x] UPDATE
  - [x] DELETE

## License

MIT - see [LICENSE.md](LICENSE.md).
