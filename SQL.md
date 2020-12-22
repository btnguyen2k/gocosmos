# gocosmos supported SQL statements

- Database: [CREATE DATABASE](#create-database), [DROP DATABASE](#drop-database), [LIST DATABASES](#list-databases).
- Collection: [CREATE COLLECTION](#create-collection), [DROP COLLECTION](#drop-collection), [LIST COLLECTIONS](#list-collections).
- Document: [INSERT](#insert), [UPSERT](#upsert), [UPDATE](#update), [DELETE](#delete), [SELECT](#select).

## Database

Suported statements: `CREATE DATABASE`, `DROP DATABASE`, `LIST DATABASES`.

#### CREATE DATABASE

Summary: create a new database.

Syntax: `CREATE DATABASE [IF NOT EXISTS] <db-name> [WITH RU|MAXRU=<ru>]`.

- This statement returns error (StatusCode=409) if the specified database already existed. If `IF NOT EXISTS` is specified, the error is silently ignored.
- Provisioned capacity can be optionally specified via `WITH RU=<ru>` or `WITH MAXRU=<ru>`.

Example:
```go
_, err := db.Exec("CREATE DATABASE IF NOT EXISTS mydb WITH ru=400")
if err != nil {
    panic(err)
}
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

[Back to top](#top)

#### DROP DATABASE

Summary: delete an existing database.

Syntax: `DROP DATABASE [IF EXISTS] <db-name>`.

- This statement returns error (StatusCode=404) if the specified database does not exist. If `IF EXISTS` is specified, the error is silently ignored.

Example:
```go
_, err := db.Exec("DROP DATABASE IF EXISTS mydb")
if err != nil {
    panic(err)
}
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

[Back to top](#top)

#### LIST DATABASES

Summary: list all existing databases.

Syntax: `LIST DATABASES`.

Example:
```go
dbRows, err := db.Query("LIST DATABASES")
if err != nil {
    panic(err)
}

colTypes, err := dbRows.ColumnTypes()
if err != nil {
    panic(err)
}
numCols := len(colTypes)
for dbRows.Next() {
    vals := make([]interface{}, numCols)
    scanVals := make([]interface{}, numCols)
    for i := 0; i < numCols; i++ {
        scanVals[i] = &vals[i]
    }
    if err := dbRows.Scan(scanVals...); err == nil {
        row := make(map[string]interface{})
        for i, v := range colTypes {
            row[v.Name()] = vals[i]
        }
        fmt.Println("Database:", row)
    } else if err != sql.ErrNoRows {
        panic(err)
    }
}
```

> Use `sql.DB.Query` to execute the statement, `Exec` will return error.

[Back to top](#top)

## Collection

Suported statements: `CREATE COLLECTION`, `DROP COLLECTION`, `LIST COLLECTIONS`.

#### CREATE COLLECTION

Summary: create a new collection.

Alias: `CREATE TABLE`.

Syntax: `CREATE COLLECTION [IF NOT EXISTS] [<db-name>.]<collection-name> <WITH [LARGE]PK=partitionKey> [WITH RU|MAXRU=ru] [WITH UK=/path1:/path2,/path3;/path4]`.

- This statement returns error (StatusCode=409) if the specified collection already existed. If `IF NOT EXISTS` is specified, the error is silently ignored.
- Partition key must be specified using `WITH pk=<partition-key>`. If partition key is larger than 100 bytes, use `largepk` instead.
- Provisioned capacity can be optionally specified via `WITH RU=<ru>` or `WITH MAXRU=<ru>`.
- Unique keys are optionally specified via `WITH uk=/uk1_path:/uk2_path1,/uk2_path2:/uk3_path`. Each unique key is a comma-separated list of paths (e.g. `/uk_path1,/uk_path2`); unique keys are separated by colons (e.g. `/uk1:/uk2:/uk3`).

Example:
```go
_, err := db.Exec("CREATE COLLECTION IF NOT EXISTS mydb.mytable WITH pk=/username WITH ru=400 WITH uk=/email")
if err != nil {
    panic(err)
}
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

[Back to top](#top)

#### DROP COLLECTION

Summary: delete an existing collection.

Alias: `DROP TABLE`.

Syntax: `DROP COLLECTION [IF EXISTS] [<db-name>.]<collection-name>`.

- This statement returns error (StatusCode=404) if the specified collection does not exist. If `IF EXISTS` is specified, the error is silently ignored.

Example:
```go
_, err := db.Exec("DROP COLLECTION IF EXISTS mydb.mytable")
if err != nil {
    panic(err)
}
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

[Back to top](#top)

#### LIST COLLECTIONS

Summary: list all existing collections in a database.

Alias: `LIST TABLES`.

Syntax: `LIST COLLECTIONS [FROM <db-name>]`.

Example:
```go
dbRows, err := db.Query("LIST COLLECTIONS FROM mydb")
if err != nil {
    panic(err)
}

colTypes, err := dbRows.ColumnTypes()
if err != nil {
    panic(err)
}
numCols := len(colTypes)
for dbRows.Next() {
    vals := make([]interface{}, numCols)
    scanVals := make([]interface{}, numCols)
    for i := 0; i < numCols; i++ {
        scanVals[i] = &vals[i]
    }
    if err := dbRows.Scan(scanVals...); err == nil {
        row := make(map[string]interface{})
        for i, v := range colTypes {
            row[v.Name()] = vals[i]
        }
        fmt.Println("Collection:", row)
    } else if err != sql.ErrNoRows {
        panic(err)
    }
}
```

> Use `sql.DB.Query` to execute the statement, `Exec` will return error.

[Back to top](#top)

## Document

Suported statements: `INSERT`, `UPSERT`, `UPDATE`, `DELETE`, `SELECT`.

#### INSERT

Summary: insert a new document into an existing collection.

Syntax: `INSERT INTO [<db-name>.]<collection-name> (<field1>, <field2>,...<fieldN>) VALUES (<value1>, <value2>,...<valueN>)`.

A value is either:
- a placeholder
- a `null`
- a number
- a boolean (`true/false`)
- a string (wrapped by double quotes) that must be a valid JSON:
  - a string value in JSON (include the double quotes), for example `"\"a string\""`
  - a number value in JSON (include the double quotes), for example `"123"`
  - a boolean value in JSON (include the double quotes), for example `"true"`
  - a null value in JSON (include the double quotes), for example `"null"`
  - a map value in JSON (include the double quotes), for example `"{\"key\":\"value\"}"`
  - a list value in JSON (include the double quotes), for example `"[1,true,null,\"string\"]"`

> Placeholder is a number prefixed by `$` or `@` or `:`, for example `$1`, `@2` or `:3`. The first placeholder is 1, the second one is 2 and so on.

Example:
```go
sql := `INSERT INTO mydb.mytable (a, b, c, d, e) VALUES (1, "\"a string\"", true, "[1,true,null,\"string\"]", $1)`
result, err := db.Exec(sql, map[string]interface{}{"key":"value"}, "mypk")
if err != nil {
    panic(err)
}

numRows, err := result.RowsAffected()
if err != nil {
    panic(err)
}
fmt.Println("Number of rows affected:", numRows)
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

> Value of partition key _must_ be supplied at the last argument of `db.Exec()` call.

[Back to top](#top)

#### UPSERT

Summary: insert a new document or replace an existing one.

Syntax & Usage: see [INSERT](#insert).

[Back to top](#top)

#### DELETE

Summary: delete an existing document.

Syntax: `DELETE FROM [<db-name>.]<collection-name> WHERE id=<id-value>`

- `DELETE` removes only one document specified by id.
- `<id-value>` is treated as string, i.e. `WHERE id=abc` has the same effect as `WHERE id="abc"`. A placeholder can be use as `<id-value>`.

> Placeholder is a number prefixed by `$` or `@` or `:`, for example `$1`, `@2` or `:3`. The first placeholder is 1, the second one is 2 and so on.

Example:
```go
sql := `DELETE FROM mydb.mytable WHERE id=@1`
result, err := db.Exec(sql, "myid", "mypk")
if err != nil {
    panic(err)
}

numRows, err := result.RowsAffected()
if err != nil {
    panic(err)
}
fmt.Println("Number of rows affected:", numRows)
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

> Value of partition key _must_ be supplied at the last argument of `db.Exec()` call.

[Back to top](#top)

#### UPDATE

Summary: update an existing document.

Syntax: `UPDATE [<db-name>.]<collection-name> SET <fiel1>=<value1>,<field2>=<value2>,...<fieldN>=<valueN>, WHERE id=<id-value>`

- `UPDATE` modifies only one document specified by id.
- `<id-value>` is treated as string, i.e. `WHERE id=abc` has the same effect as `WHERE id="abc"`. A placeholder can be use as `<id-value>`.
- A value is either:
  - a placeholder
  - a `null`
  - a number
  - a boolean (`true/false`)
  - a string (wrapped by double quotes) that must be a valid JSON:
    - a string value in JSON (include the double quotes), for example `"\"a string\""`
    - a number value in JSON (include the double quotes), for example `"123"`
    - a boolean value in JSON (include the double quotes), for example `"true"`
    - a null value in JSON (include the double quotes), for example `"null"`
    - a map value in JSON (include the double quotes), for example `"{\"key\":\"value\"}"`
    - a list value in JSON (include the double quotes), for example `"[1,true,null,\"string\"]"`

> Placeholder is a number prefixed by `$` or `@` or `:`, for example `$1`, `@2` or `:3`. The first placeholder is 1, the second one is 2 and so on.

Example:
```go
sql := `UPDATE mydb.mytable SET a=1, b="\"a string\"", c=true, d="[1,true,null,\"string\"]", e=:2 WHERE id=@1`
result, err := db.Exec(sql, "myid", map[string]interface{}{"key":"value"}, "mypk")
if err != nil {
    panic(err)
}

numRows, err := result.RowsAffected()
if err != nil {
    panic(err)
}
fmt.Println("Number of rows affected:", numRows)
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

> Value of partition key _must_ be supplied at the last argument of `db.Exec()` call.

[Back to top](#top)

#### SELECT

Summary: query documents in a collection.

Syntax: `SELECT [CROSS PARTITION] ... FROM <collection-name> ... [WITH database=<db-name>] [WITH collection=<collection-name>] [WITH cross_partition=true]`

The `SELECT` query follows [Azure Cosmos DB's SQL grammar](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-select) with a few extensions:
- If the collection is partitioned, specify `CROSS PARTITION` to allow execution across multiple partitions. This clause is not required if query is to be executed on a single partition. Cross-partition execution can also be enabled using `WITH cross_partition=true`.
- The database on which the query is execute _must_ be specified via `WITH database=<db-name>` or `WITH db=<db-name>` or with default database option via DSN.
- The collection to query from can be optionally specified via `WITH collection=<coll-name>` or `WITH table=<coll-name>`. If not specified, the collection name is extracted from the `FROM <collection-name>` clause.
- Placeholder syntax: `@i`, `$i` or `:i` (where i denotes the i-th parameter, the first parameter is 1).

Example: single partition, collection name is extracted from the `FROM...` clause
```go
sql := `SELECT * FROM mytable c WHERE c.age>@1 AND c.class=$2 AND c.pk="\"mypk\"" WITH db=mydb`
dbRows, err := db.Query(sql, 21, "Grade A")
if err != nil {
    panic(err)
}

colTypes, err := dbRows.ColumnTypes()
if err != nil {
    panic(err)
}
numCols := len(colTypes)
for dbRows.Next() {
    vals := make([]interface{}, numCols)
    scanVals := make([]interface{}, numCols)
    for i := 0; i < numCols; i++ {
        scanVals[i] = &vals[i]
    }
    if err := dbRows.Scan(scanVals...); err == nil {
        row := make(map[string]interface{})
        for i, v := range colTypes {
            row[v.Name()] = vals[i]
        }
        fmt.Println("Collection:", row)
    } else if err != sql.ErrNoRows {
        panic(err)
    }
}
```

Example: cross partition, collection name is explicitly specified via `WITH...` clause
```go
sql := `SELECT CROSS PARTITION * FROM c WHERE c.age>@1 AND c.active=true WITH db=mydb WITH table=mytable`
dbRows, err := db.Query(sql, 21)
if err != nil {
    panic(err)
}
```

> Use `sql.DB.Query` to execute the statement, `Exec` will return error.

[Back to top](#top)
