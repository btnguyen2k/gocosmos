# gocosmos - Release notes

## 2024-02-13 - v1.1.0

### Added/Refactoring

- Refactored DELETE statement: appending PK values at the end of parameter list is no longer needed.
- Refactored UPDATE statement: appending PK values at the end of parameter list is no longer needed.
- Feature: INSERT/UPSERT statement accepts WITH PK clause. Appending PK values at the end of parameter list is no longer needed.

### Deprecated

- Deprecated: WITH singlePK/SINGLE_PK is now deprecated for INSERT/UPSERT, DELETE and UPDATE statements.

### Fixed/Improvement

- Improvement: implement fmt.Stringer
- Improvement: Conn implements interface driver.Pinger
- Improvement: Driver implements interface driver.DriverContext
- Improvement: StmtCreateCollection/StmtAlterCollection/StmtDropCollection implements interface driver.StmtExecContext
- Improvement: StmtListCollections implements interface driver.StmtQueryContext
- Improvement: StmtCreateDatabase/StmtAlterDatabase/StmtDropDatabase implements interface driver.StmtExecContext
- Improvement: StmtListDatabases implements interface driver.StmtQueryContext
- Improvement: StmtInsert/StmtDelete/StmtUpdate implements interface driver.StmtExecContext
- Improvement: StmtSelect implements interface driver.StmtQueryContext

## 2023-12-22 - v1.0.0

### Changed

- BREAKING: typo fixed, change struct RestReponse to RestResponse
- BREAKING: bump GO version to v1.18

### Added/Refactoring

- Refactor: remove internal sync.Mutex from OfferInfo
- Add methods GetApiVersion/GetAutoId/SetAutoId to RestClient struct
- Refactor: follow go-module-template
- Refactor: move tests to separated subpackage

### Fixed/Improvement

- Fix CodeQL alerts
- Dependency: bump github.com/btnguyen2k/consu/checksum to v1.1.0
- Fix: server may return no content with http status 204 or 304
- Fix: golang-lint

## 2023-06-16 - v0.3.0

- Change default API version to `2020-07-15`.
- Add [Hierarchical Partition Keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) (sub-partitions) support.
- Use PartitionKey version 2 (replacing version 1), hence large PK is always enabled.

## 2023-06-09 - v0.2.1

- Bug fixes, Refactoring & Enhancements.

## 2023-03-14 - v0.2.0

- `RestClient`:
  - `QueryDocuments`: enhancements & bug fixed with cross-partition queries.
  - New function `QueryDocumentsCrossPartition(QueryReq) *RespQueryDocs` do address limitations of `QueryDocuments`.
- `database/sql` driver:
  - Update `StmtSelect.Query` to better support cross-partition queries.

## 2023-01-04 - v0.1.9

- Update `RestClient`
  - `QueryDocuments`: better support cross-partition queries.
  - `ListDocuments`: support fetching change feed.

## 2022-12-04 - v0.1.8

- REST client: rewrite `RestClient.QueryDocuments`. TODO:
  - [x] (v0.1.7+) simple cross-partition queries (+paging)
  - [-] cross-partition queries with ordering (+paging) / partial supported if number of pkrange == 1
  - [-] cross-partition queries with group-by (+paging) / partial supported if number of pkrange == 1

## 2022-12-02 - v0.1.7

- REST client: fix a bug where function `QueryDocuments` does not return all documents if the query is cross-partition.

## 2022-02-16 - v0.1.6

- REST client & Driver for `database/sql`: fix a bug in function `ReplaceOfferForResource` caused by a change from v0.1.5.

## 2022-02-16 - v0.1.5

- Fix a bug where no-parameterized query returns error.
- Fix a bug where database or collection name contains upper-cased characters.

## 2021-07-14 - v0.1.4

- REST client & Driver for `database/sql`:
  - Add parameter `InsecureSkipVerify=<true/false>` to connection string. This parameter is optional, default value is `false`.
    If `true`, REST client will disable CA verification for https endpoint (useful to run against test/dev env with local/docker Cosmos DB emulator).

## 2020-12-27 - v0.1.3

- REST client: new function `GetPkranges(dbName, collName string)`.
- Support cross-partition queries & fix "The provided cross partition query can not be directly served by the gateway".

## 2020-12-26 - v0.1.2

- REST client & Driver for `database/sql`:
  - Add `auto-id` support.

## 2020-12-25 - v0.1.1

- REST client: new functions
  - `GetOfferForResource(rid string)`: get throughput info of a resource.
  - `QueryOffers(query string)`: query existing offers.
  - `ReplaceOfferForResource(rid string, ru, maxru int)`: replace/update a resource's throughput.
- Driver for `database/sql`:
  - Add default database support to DSN.
  - Add `ALTER DATABASE` and `ALTER COLLECTION` statements.

## 2020-12-21 - v0.1.0

First release:
- REST client for Azure Cosmos DB SQL API:
  - Database: `Create`, `Get`, `Delete` and `List`.
  - Collection: `Create`, `Replace`, `Get`, `Delete` and `List`.
  - Document: `Create`, `Replace`, `Get`, `Delete`, `Query` and `List`.
- Driver for `database/sql`, supported statements:
  - Database: `CREATE DATABASE`, `DROP DATABASE`, `LIST DATABASES`
  - Collection/Table: `CREATE TABLE/COLLECTION`, `DROP TABLE/COLLECTION`, `LIST TABLES/COLLECTIONS`
  - Document: `INSERT`, `UPSERT`, `SELECT`, `UPDATE`, `DELETE`
