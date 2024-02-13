# gocosmos - Supported SQL statements

- Database: [CREATE DATABASE](#create-database), [ALTER DATABASE](#alter-database), [DROP DATABASE](#drop-database), [LIST DATABASES](#list-databases).
- Collection: [CREATE COLLECTION](#create-collection), [ALTER COLLECTION](#alter-collection), [DROP COLLECTION](#drop-collection), [LIST COLLECTIONS](#list-collections).
- Document: [INSERT](#insert), [UPSERT](#upsert), [UPDATE](#update), [DELETE](#delete), [SELECT](#select).

## Database

Supported statements: `CREATE DATABASE`, `ALTER DATABASE`, `DROP DATABASE`, `LIST DATABASES`.

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

Description: change database's throughput.

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

Supported statements: `CREATE COLLECTION`, `ALTER COLLECTION`, `DROP COLLECTION`, `LIST COLLECTIONS`.

#### CREATE COLLECTION

Description: create a new collection.

Alias: `CREATE TABLE`.

Syntax:

```sql
CREATE COLLECTION [IF NOT EXISTS] [<db-name>.]<collection-name>
<WITH PK=partitionKey>
[[,] WITH RU|MAXRU=ru]
[[,] WITH UK=/path1:/path2,/path3;/path4]
```

> `<db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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
  - Hierarchical Partition Key is supported, using `WITH pk=/path1,/path2...` (up to 3 path levels).
- Provisioned capacity can be optionally specified via `WITH RU=<ru>` or `WITH MAXRU=<ru>`.
  - Only one of `RU` and `MAXRU` options should be specified, _not both_; error is returned if both optiosn are specified.
- Unique keys are optionally specified via `WITH uk=/uk1_path:/uk2_path1,/uk2_path2:/uk3_path`. Each unique key is a comma-separated list of paths (e.g. `/uk_path1,/uk_path2`); unique keys are separated by colons (e.g. `/uk1:/uk2:/uk3`).

[Back to top](#top)

#### ALTER COLLECTION

Description: change collection's throughput.

Alias: `ALTER TABLE`.

Syntax:

```sql
ALTER COLLECTION [<db-name>.]<collection-name> WITH RU|MAXRU=<ru>
```

> `<db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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

> `<db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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

> `FROM <db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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

Supported statements: `INSERT`, `UPSERT`, `UPDATE`, `DELETE`, `SELECT`.

#### INSERT

Description: insert a new document into an existing collection.

Syntax:

```sql
INSERT INTO [<db-name>.]<collection-name>
(<field1>, <field2>,...<fieldN>)
VALUES (<value1>, <value2>,...<valueN>)
[WITH PK=<partition-key>]
```

> `<db-name>` can be omitted if `DefaultDb` is supplied in the [Data Source Name (DSN)](README.md#example-usage).

Example:
```go
sql := `INSERT INTO mydb.mytable (id, a, b, c, d) VALUES (1, "\"a string\"", :1, @2, $3) WITH pk=/id`
params := []interface{}{
	true, // value for :1 
	[]interface{}{1, true, nil, "string"}, // value for @2 
	map[string]interface{}{"key":"value"}, // value for $3
}
dbresult, err := db.Exec(sql, params...)
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected()) // output 1
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error!

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

Example:
```sql
INSERT INTO mydb.mytable (
    id, 
    anull,
    anum, 
    abool, 
    jstring, 
    jnum,
    jbool,
    jnull, 
    jmap, 
    jlist) VALUES (
    :1,
    NULL,
    1.23,
    true, 
    "\"a string\"", 
    "123", 
    "true", 
    "null", 
    "{\"key1\":\"value\",\"key2\": 2.34, \"key3\": false, \"key4\": null}", 
    "[1,true,null,\"string\"]"
) WITH PK=/id
```

**Since <<VERSION>>**:

- `WITH SINGLE_PK` is deprecated and will be _removed_ in future version! Instead, use `WITH PK=/pkey` (or `WITH PK=/pkey1,/pkey2` if [Hierarchical Partition Keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) - also known as sub-partitions - is used on the collection).
- Supplying values for partition key at the end of parameter list is no longer required, but still supported for backward compatibility. This behaviour will be _removed_ in future version!

> `gocosmos` automatically discovers PK of the collection by fetching metadata from server.
> Using `WITH PK` will save one round-trip to Cosmos DB server to fetch the collection's partition key info.

[Back to top](#top)

#### UPSERT

Description: insert a new document _or replace_ an existing one.

Syntax & Usage: similar to [INSERT](#insert).

```sql
UPSERT INTO [<db-name>.]<collection-name>
(<field1>, <field2>,...<fieldN>)
VALUES (<value1>, <value2>,...<valueN>)
[WITH PK=<partition-key>]
```

[Back to top](#top)

#### DELETE

Description: delete an existing document.

Syntax:

```sql
DELETE FROM <db-name>.<collection-name>
WHERE id=<id-value>
[AND pkfield1=<pk1-value> [AND pkfield2=<pk2-value> ...]]
```

> `<db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
sql := `DELETE FROM mydb.mytable WHERE id=@1 AND pk=@2`
dbresult, err := db.Exec(sql, "myid", "mypk")
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected()) // output 1
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- The clause `WHERE id=<id-value>` is mandatory, and `id` is a keyword, _not_ a field name!
- If collection's PK has more than one path (i.e. sub-partition is used), the partition paths must be specified in the same order as in the collection (.e.g. `AND pkfield1=value1 AND pkfield2=value2...`).
- `id-value` and `pk-value` must follow the value syntax described [here](#value). Note: value for `id` should always be a string!
- `DELETE` removes _only one document_ specified by `id`.
- Upon successful execution, `RowsAffected()` returns `(1, nil)`. If no document matched, `RowsAffected()` returns `(0, nil)`.

> `gocosmos` automatically discovers PK of the collection by fetching metadata from server.
> Supplying pk-fields and pk-values is highly recommended to save one round-trip to server to fetch the collection's partition key info.

**Since <<VERSION>>**:

- `WITH SINGLE_PK` is deprecated and will be _removed_ in future version! Instead, use `AND pkfield=value` (or `AND pkfield1=value1 AND pkfield2=value2...` if [Hierarchical Partition Keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) - also known as sub-partitions - is used on the collection).
- Supplying values for partition key at the end of parameter list is no longer required, but still supported for backward compatibility. This behaviour will be _removed_ in future version!

[Back to top](#top)

#### UPDATE

Description: update an existing document.

Syntax:

```sql
UPDATE [<db-name>.]<collection-name>
SET <fiel1>=<value1>[,<field2>=<value2>,...<fieldN>=<valueN>]
WHERE id=<id-value>
[AND pkfield1=<pk1-value> [AND pkfield2=<pk2-value> ...]]
```

> `<db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

Example:
```go
sql := `UPDATE mydb.mytable SET a=1, b="\"a string\"", c=true, d="[1,true,null,\"string\"]", e=:2 WHERE id=@1 AND pk=$3`
dbresult, err := db.Exec(sql, "myid", map[string]interface{}{"key":"value"}, "mypk") // note: id is $1 and value for field e is :2
if err != nil {
	panic(err)
}
fmt.Println(dbresult.RowsAffected())
```

> Use `sql.DB.Exec` to execute the statement, `Query` will return error.

- The clause `WHERE id=<id-value>` is mandatory, and `id` is a keyword, _not_ a field name!
- If collection's PK has more than one path (i.e. sub-partition is used), the partition paths must be specified in the same order as in the collection (.e.g. `AND pkfield1=value1 AND pkfield2=value2...`).
- `id-value` and `pk-value` must follow the value syntax described [here](#value). Note: value for `id` should always be a string!
- `UPDATE` modifies _only one document_ specified by `id`.
- Upon successful execution, `RowsAffected()` returns `(1, nil)`. If no document matched, `RowsAffected()` returns `(0, nil)`.

> `gocosmos` automatically discovers PK of the collection by fetching metadata from server.
> Supplying pk-fields and pk-values is highly recommended to save one round-trip to server to fetch the collection's partition key info.

**Since <<VERSION>>**:

- `WITH SINGLE_PK` is deprecated and will be _removed_ in future version! Instead, use `AND pkfield=value` (or `AND pkfield1=value1 AND pkfield2=value2...` if [Hierarchical Partition Keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) - also known as sub-partitions - is used on the collection).
- Supplying values for partition key at the end of parameter list is no longer required, but still supported for backward compatibility. This behaviour will be _removed_ in future version!

[Back to top](#top)

#### SELECT

Description: query documents in a collection.

Syntax:

```sql
SELECT [CROSS PARTITION] ... FROM <collection-name> ...
[WITH database=<db-name>]
[[,] WITH collection=<collection-name>]
[[,] WITH cross_partition|CrossPartition[=true]]
```

> `<db-name>` can be omitted if `DefaultDb` is supplied in the Data Source Name (DSN).

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
- The database on which the query is executed _must_ be specified via `WITH database=<db-name>` or `WITH db=<db-name>` or with default database option via DSN.
- The collection to query from can be optionally specified via `WITH collection=<coll-name>` or `WITH table=<coll-name>`. If not specified, the collection name is extracted from the `FROM <collection-name>` clause.
- See [here](#value) for more details on values and placeholders.

[Back to top](#top)
