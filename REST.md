# gocosmos - REST client

`gocosmos` driver uses its own REST client to communicate with Azure Cosmos DB SQL API. The REST client can be used as a standalone package.

The REST client supports:
- Database: `Create`, `Get`, `Delete`, `List` commands and changing throughput.
- Collection: `Create`, `Replace`, `Get`, `Delete`, `List` commands and changing throughput.
- Document: `Create`, `Replace`, `Get`, `Delete`, `Query` and `List` commands.

### Example usage:

```go
package main

import (
	"github.com/btnguyen2k/gocosmos"
)

func main() {
	cosmosDbConnStr := "AccountEndpoint=https://localhost:8081/;AccountKey=<cosmosdb-account-key>"
	client, err := gocosmos.NewRestClient(nil, cosmosDbConnStr)
	if err != nil {
		panic(err)
	}

	dbSpec := gocosmos.DatabaseSpec{Id: "mydb", Ru: 400}
	result := client.CreateDatabase(dbSpec)
	if result.Error() != nil {
		panic(result.Error())
	}

	// database "mydb" has been created successfully
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
- `AutoId`: (optional, available since [v0.1.2](RELEASE-NOTES.md)) see [auto id](README.md#auto-id) session.
- `InsecureSkipVerify`: (optional, available since [v0.1.4](RELEASE-NOTES.md)) if `true`, disable CA verification for https endpoint (useful to run against test/dev env with local/docker Cosmos DB emulator).

### Known issues

**`GROUP BY` combined with `ORDER BY` is not supported**

Azure Cosmos DB does not support `GROUP BY` combined with `ORDER BY` yet. You will receive the following error message:

> 'ORDER BY' is not supported in presence of GROUP BY.

**Cross-partition queries**

When documents are spanned across partitions, they must be fetched from multiple `PkRangeId`s and then merged to build
the final result. Due to this behaviour, some cross-partition queries might not work as expected:

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
