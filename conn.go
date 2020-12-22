package gocosmos

import (
	"database/sql/driver"
	"errors"
	"time"
)

var (
	locGmt, _ = time.LoadLocation("GMT")
)

// Conn is Azure CosmosDB connection handle.
type Conn struct {
	restClient *RestClient // Azure CosmosDB REST API client.
	defaultDb  string      // default database used in Cosmos DB operations.
}

// Prepare implements driver.Conn.Prepare.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return parseQueryWithDefaultDb(c, c.defaultDb, query)
}

// Close implements driver.Conn.Close.
func (c *Conn) Close() error {
	return nil
}

// Begin implements driver.Conn.Begin.
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("transaction is not supported")
}

// CheckNamedValue implements driver.NamedValueChecker.CheckNamedValue.
func (c *Conn) CheckNamedValue(value *driver.NamedValue) error {
	// since CosmosDB is document db, it accepts any value types
	return nil
}
