package go_cosmos

import (
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/gjrc"
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

// Open implement driver.Driver.Open.
//
// connection string is expected in the following format:
// AccountEndpoint=<cosmosdb-http-endpoint>;AccountKey=<account-key>;OtherSettingKey=OtherSettingValue;...
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	params := make(map[string]string)
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		tokens := strings.SplitN(part, "=", 2)
		if len(tokens) != 2 {
			return nil, errors.New("invalid connection string")
		}
		params[tokens[0]] = tokens[1]
	}
	endpoint := strings.TrimSuffix(params["AccountEndpoint"], "/")
	if endpoint == "" {
		return nil, errors.New("AccountEndpoint not found in connection string")
	}
	accountKey := params["AccountKey"]
	if accountKey == "" {
		return nil, errors.New("AccountKey not found in connection string")
	}
	delete(params, "AccountEndpoint")
	delete(params, "AccountKey")

	key, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return nil, fmt.Errorf("cannot base64 decode account key: %s", err)
	}
	conn := &Conn{
		client:   gjrc.NewGjrc(nil, 10*time.Second),
		endpoint: endpoint,
		authKey:  key,
		params:   params,
	}
	return conn, nil
}
