package gocosmos

import (
	"database/sql/driver"
	"time"
)

var (
	locGmt, _ = time.LoadLocation("GMT")
)

// Conn is Azure CosmosDB connection handle.
type Conn struct {
	restClient *RestClient       // Azure CosmosDB REST API client.
	endpoint   string            // Azure CosmosDB endpoint
	authKey    []byte            // Account key to authenticate
	params     map[string]string // other parameters
}

// Close implements driver.Conn.Prepare.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return parseQuery(c, query)
}

// Close implements driver.Conn.Close.
func (c *Conn) Close() error {
	return nil
}

// Close implements driver.Conn.Begin.
func (c *Conn) Begin() (driver.Tx, error) {
	panic("implement me")
}

// CheckNamedValue implements driver.NamedValueChecker.CheckNamedValue.
func (c *Conn) CheckNamedValue(value *driver.NamedValue) error {
	// since CosmosDB is document db, it accepts any value types
	return nil
}
