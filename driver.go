package gocosmos

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

func init() {
	sql.Register("gocosmos", &Driver{})
}

var (
	// ErrForbidden is returned when the operation is not allowed on the target resource.
	ErrForbidden = errors.New("StatusCode=403 Forbidden")

	// ErrNotFound is returned when target resource can not be found.
	ErrNotFound = errors.New("StatusCode=404 Not Found")

	// ErrConflict is returned when the executing operation cause conflict (e.g. duplicated id).
	ErrConflict = errors.New("StatusCode=409 Conflict")
)

// Driver is Azure CosmosDB driver for database/sql.
type Driver struct {
}

// Open implements driver.Driver.Open.
//
// connStr is expected in the following format:
// AccountEndpoint=<cosmosdb-restapi-endpoint>;AccountKey=<account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>]
// If not supplied, default value for TimeoutMs is 10 seconds and Version is "2018-12-31".
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	restClient, err := NewRestClient(nil, connStr)
	if err != nil {
		return nil, err
	}
	return &Conn{restClient: restClient}, nil
}
