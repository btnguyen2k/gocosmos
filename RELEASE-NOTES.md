# gocosmos release notes

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
