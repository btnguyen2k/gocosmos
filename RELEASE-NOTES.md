# gocosmos release notes

## 2020-12-2x - v0.1.1

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
