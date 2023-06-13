# gocosmos - Supported SQL statements
    
- Database: [CREATE DATABASE](#create-database), [ALTER DATABASE](#alter-database), [DROP DATABASE](#drop-database), [LIST DATABASES](#list-databases).
- Collection: [CREATE COLLECTION](#create-collection), [ALTER COLLECTION](#alter-collection), [DROP COLLECTION](#drop-collection), [LIST COLLECTIONS](#list-collections).
- Document: [INSERT](#insert), [UPSERT](#upsert), [UPDATE](#update), [DELETE](#delete), [SELECT](#select).

## Database

Suported statements: `CREATE DATABASE`, `ALTER DATABASE`, `DROP DATABASE`, `LIST DATABASES`.

#### CREATE DATABASE

Description: create a new database.

Syntax:

```sql
CREATE DATABASE [IF NOT EXISTS] <db-name> [WITH RU|MAXRU=<ru>]
```

Example:
```go
dbresult, err := db.Exec("CREATE DATABASE IF NOT EXISTS mydb WITH ru=400")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- Upon successful execution, `RowsAffected()` returns `(1, nil)`.
- This statement returns error `ErrConflict` if the specified database already existed. If `IF NOT EXISTS` is specified, `RowsAffected()` returns `(0, nil)`.
- Provisioned capacity can be optionally specified via `WITH RU=<ru>` or `WITH MAXRU=<ru>`.
  - If none of `RU` or `MAXRU` is provided, the new database is created without provisioned capacity, and this _cannot_ be changed latter.
  - Only one of `RU` and `MAXRU` options should be specified, _not both_; error is returned if both optiosn are specified.

[Back to top](#top)

#### ALTER DATABASE

Description: change database's throughput (since [v0.1.1](RELEASE-NOTES.md)).

Syntax:

```sql
ALTER DATABASE <db-name> WITH RU|MAXRU=<ru>
```

Example:
```go
dbresult, err := db.Exec("ALTER DATABASE mydb WITH ru=400")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- Upon successful execution, `RowsAffected()` returns `(1, nil)`.
- This statement returns error `ErrNotFound` if the specified database does not exist.
- Only one of `RU` and `MAXRU` options should be specified, _not both_; error is returned if both optiosn are specified.

[Back to top](#top)

#### DROP DATABASE

Description: delete an existing database.

Syntax:

```sql
DROP DATABASE [IF EXISTS] <db-name>
```

Example:
```go
dbresult, err := db.Exec("DROP DATABASE IF EXISTS mydb")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- Upon successful execution, `RowsAffected()` returns `(1, nil)`.
- This statement returns error `ErrNotFound` if the specified database does not exist. If `IF EXISTS` is specified, `RowsAffected()` returns `(0, nil)`.

[Back to top](#top)

#### LIST DATABASES

Description: list all existing databases.

Syntax: 

```sql
LIST DATABASES
```

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

Suported statements: `CREATE COLLECTION`, `ALTER COLLECTION`, `DROP COLLECTION`, `LIST COLLECTIONS`.

#### CREATE COLLECTION

Description: create a new collection.

Alias: `CREATE TABLE`.

Syntax: 

```sql
CREATE COLLECTION [IF NOT EXISTS] [<db-name>.]<collection-name>
<WITH [LARGE]PK=partitionKey>
[[,] WITH RU|MAXRU=ru]
[[,] WITH UK=/path1:/path2,/path3;/path4]
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
dbresult, err := db.Exec("CREATE COLLECTION IF NOT EXISTS mydb.mytable WITH pk=/username WITH ru=400 WITH uk=/email")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- Upon successful execution, `RowsAffected()` returns `(1, nil)`.
- This statement returns error `ErrConflict` if the specified collection already existed. If `IF NOT EXISTS` is specified, `RowsAffected()` returns `(0, nil)`.
- Partition key must be specified using `WITH pk=<partition-key>`.
  - Since [v0.3.0](RELEASE-NOTES.md), large pk is always enabled, `WITH largepk` is for backward compatibility only.
  - Since [v0.3.0](RELEASE-NOTES.md), Hierarchical Partition Key is supported, using `WITH pk=/path1,/path2...` (up to 3 path levels).
- Provisioned capacity can be optionally specified via `WITH RU=<ru>` or `WITH MAXRU=<ru>`.
  - Only one of `RU` and `MAXRU` options should be specified, _not both_; error is returned if both optiosn are specified.
- Unique keys are optionally specified via `WITH uk=/uk1_path:/uk2_path1,/uk2_path2:/uk3_path`. Each unique key is a comma-separated list of paths (e.g. `/uk_path1,/uk_path2`); unique keys are separated by colons (e.g. `/uk1:/uk2:/uk3`).

[Back to top](#top)

#### ALTER COLLECTION

Description: change collection's throughput (since [v0.1.1](RELEASE-NOTES.md)).

Alias: `ALTER TABLE`.

Syntax:

```sql
ALTER COLLECTION [<db-name>.]<collection-name> WITH RU|MAXRU=<ru>
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
dbresult, err := db.Exec("ALTER COLLECTION mydb.mytable WITH ru=400")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- Upon successful execution, `RowsAffected()` returns `(1, nil)`.
- This statement returns error `ErrNotFound` if the specified database does not exist.
- Only one of `RU` and `MAXRU` options should be specified, _not both_; error is returned if both optiosn are specified.

[Back to top](#top)

#### DROP COLLECTION

Description: delete an existing collection.

Alias: `DROP TABLE`.

Syntax:

```sql
DROP COLLECTION [IF EXISTS] [<db-name>.]<collection-name>
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
dbresult, err := db.Exec("DROP COLLECTION IF EXISTS mydb.mytable")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- Upon successful execution, `RowsAffected()` returns `(1, nil)`.
- This statement returns error `ErrNotFound` if the specified database does not exist. If `IF EXISTS` is specified, `RowsAffected()` returns `(0, nil)`.

[Back to top](#top)

#### LIST COLLECTIONS

Description: list all existing collections in a database.

Alias: `LIST TABLES`.

Syntax:

```sql
LIST COLLECTIONS [FROM <db-name>]
```

> `FROM <db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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

Description: insert a new document into an existing collection.

Syntax:

```sql
INSERT INTO [<db-name>.]<collection-name>
(<field1>, <field2>,...<fieldN>)
VALUES (<value1>, <value2>,...<valueN>)
[WITH singlePK|SINGLE_PK]
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Since [v0.3.0](RELEASE-NOTES.md), `gocosmos` supports [Hierarchical Partition Keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) (or sub-partitions). If the collection is known not to have sub-partitions, supplying `WITH singlePK` (or `WITH SINGLE_PK`) can save one roundtrip to Cosmos DB server.

Example:
```go
sql := `INSERT INTO mydb.mytable (a, b, c, d, e) VALUES (1, "\"a string\"", :1, @2, $3)`
dbresult, err := db.Exec(sql, true, []interface{}{1, true, nil, "string"}, map[string]interface{}{"key":"value"}, "mypk")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

> Values of partition key _must_ be supplied at the end of the argument list when invoking `db.Exec()`.

<a id="value"></a>A value is either:
- a placeholder - which is a number prefixed by `$` or `@` or `:`, for example `$1`, `@2` or `:3`. Placeholders are 1-based index, that means starting from 1.
- a `null`
- a number, for example `12.3`.
- a boolean (`true/false`)
- a JSON string (wrapped by double quotes), must be a valid JSON:
  - a string value in JSON (include the double quotes), for example `"\"a string\""`
  - a number value in JSON (include the double quotes), for example `"123"`
  - a boolean value in JSON (include the double quotes), for example `"true"`
  - a null value in JSON (include the double quotes), for example `"null"`
  - a map value in JSON (include the double quotes), for example `"{\"key\":\"value\"}"`
  - a list value in JSON (include the double quotes), for example `"[1,true,null,\"string\"]"`

[Back to top](#top)

#### UPSERT

Description: insert a new document or replace an existing one.

Syntax & Usage: similar to [INSERT](#insert).

```sql
UPSERT INTO [<db-name>.]<collection-name>
(<field1>, <field2>,...<fieldN>)
VALUES (<value1>, <value2>,...<valueN>)
[WITH singlePK|SINGLE_PK]
```

[Back to top](#top)

#### DELETE

Description: delete an existing document.

Syntax:

```sql
DELETE FROM [<db-name>.]<collection-name> WHERE id=<id-value>
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
sql := `DELETE FROM mydb.mytable WHERE id=@1`
dbresult, err := db.Exec(sql, "myid", "mypk")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

> Value of partition key _must_ be supplied at the last argument of `db.Exec()` call.

- `DELETE` removes only one document specified by id.
- Upon successful execution, `RowsAffected()` returns `(1, nil)`. If no document matched, `RowsAffected()` returns `(0, nil)`.
- `<id-value>` is treated as string, i.e. `WHERE id=abc` has the same effect as `WHERE id="abc"`. A placeholder can be used in the place of `<id-value>`. See [here](#value) for more details on values and placeholders.

[Back to top](#top)

#### UPDATE

Description: update an existing document.

Syntax:

```sql
UPDATE [<db-name>.]<collection-name> SET <fiel1>=<value1>[,<field2>=<value2>,...<fieldN>=<valueN>] WHERE id=<id-value>
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
sql := `UPDATE mydb.mytable SET a=1, b="\"a string\"", c=true, d="[1,true,null,\"string\"]", e=:2 WHERE id=@1`
dbresult, err := db.Exec(sql, "myid", map[string]interface{}{"key":"value"}, "mypk")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

> Value of partition key _must_ be supplied at the last argument of `db.Exec()` call.

- `UPDATE` modifies only one document specified by id.
- Upon successful execution, `RowsAffected()` returns `(1, nil)`. If no document matched, `RowsAffected()` returns `(0, nil)`.
- `<id-value>` is treated as string, i.e. `WHERE id=abc` has the same effect as `WHERE id="abc"`. A placeholder can be used in the place of `<id-value>`.
- See [here](#value) for more details on values and placeholders.

[Back to top](#top)

#### SELECT

Description: query documents in a collection.

Syntax:

```sql
SELECT [CROSS PARTITION] ... FROM <collection-name> ...
[WITH database=<db-name>]
[[,] WITH collection=<collection-name>]
[[,] WITH cross_partition=true]
```

> `<db-name>` can be ommitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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
		fmt.Println("Row:", row)
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

The `SELECT` query follows [Azure Cosmos DB's SQL grammar](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-select) with a few extensions:
- If the collection is partitioned, specify `CROSS PARTITION` to allow execution across multiple partitions. This clause is not required if query is to be executed on a single partition. Cross-partition execution can also be enabled using `WITH cross_partition=true`.
- The database on which the query is execute _must_ be specified via `WITH database=<db-name>` or `WITH db=<db-name>` or with default database option via DSN.
- The collection to query from can be optionally specified via `WITH collection=<coll-name>` or `WITH table=<coll-name>`. If not specified, the collection name is extracted from the `FROM <collection-name>` clause.
- See [here](#value) for more details on values and placeholders.

[Back to top](#top)
