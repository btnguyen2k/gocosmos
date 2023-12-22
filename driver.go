package gocosmos

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/olaf"
)

var idGen *olaf.Olaf

func _myCurrentIp() (string, error) {
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, address := range addrs {
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					return ipnet.IP.String(), nil
				}
			}
		}
	}
	return "", errors.New("cannot fetch local IP")
}

func _myMacAddr(ip string) (net.HardwareAddr, error) {
	if interfaces, err := net.Interfaces(); err == nil {
		for _, interf := range interfaces {
			if addrs, err := interf.Addrs(); err == nil {
				for _, addr := range addrs {
					if strings.HasPrefix(addr.String(), ip+"/") {
						return interf.HardwareAddr, nil
					}
				}
			}
		}
	}
	return nil, errors.New("cannot fetch interface info for IP " + ip)
}

func init() {
	idGen = olaf.NewOlaf(time.Now().UnixNano())
	if myCurrentIp, err := _myCurrentIp(); err == nil {
		if myMacAddr, err := _myMacAddr(myCurrentIp); err == nil {
			for len(myMacAddr) < 8 {
				myMacAddr = append([]byte{0}, myMacAddr...)
			}
			// log.Printf("[DEBUG] gocosmos - Local IP: %s / MAC: %s", myCurrentIp, myMacAddr)
			idGen = olaf.NewOlaf(int64(binary.BigEndian.Uint64(myMacAddr)))
		}
	}
	sql.Register("gocosmos", &Driver{})
}

var (
	// ErrForbidden is returned when the operation is not allowed on the target resource.
	ErrForbidden = errors.New("StatusCode=403 Forbidden")

	// ErrNotFound is returned when target resource can not be found.
	ErrNotFound = errors.New("StatusCode=404 Not Found")

	// ErrConflict is returned when the executing operation cause conflict (e.g. duplicated id).
	ErrConflict = errors.New("StatusCode=409 Conflict")

	// ErrPreconditionFailure is returned when operation specified an eTag that is different from the version available
	// at the server, that is, an optimistic concurrency error.
	//
	// @Available since v0.2.1
	ErrPreconditionFailure = errors.New("StatusCode=412 Precondition failure")

	// ErrOperationNotSupported is returned to indicate that the operation is not supported.
	//
	// @Available since v0.2.1
	ErrOperationNotSupported = errors.New("this operation is not supported")

	// ErrExecNotSupported is returned to indicate that the Exec/ExecContext operation is not supported.
	//
	// @Available since v0.2.1
	ErrExecNotSupported = errors.New("this operation is not supported, please use Query")

	// ErrQueryNotSupported is returned to indicate that the Query/QueryContext operation is not supported.
	//
	// @Available since v0.2.1
	ErrQueryNotSupported = errors.New("this operation is not supported, please use Exec")
)

// Driver is Azure Cosmos DB implementation of driver.Driver.
type Driver struct {
}

// Open implements driver.Driver/Open.
//
// connStr is expected in the following format:
//
//	AccountEndpoint=<cosmosdb-restapi-endpoint>;AccountKey=<account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>][;DefaultDb=<db-name>][;AutoId=<true/false>][;InsecureSkipVerify=<true/false>]
//
// If not supplied, default value for TimeoutMs is 10 seconds, Version is DefaultApiVersion (which is "2020-07-15"), AutoId is true, and InsecureSkipVerify is false
//
// - DefaultDb is added since v0.1.1
// - AutoId is added since v0.1.2
// - InsecureSkipVerify is added since v0.1.4
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	restClient, err := NewRestClient(nil, connStr)
	if err != nil {
		return nil, err
	}
	defaultDb, ok := restClient.params["DEFAULTDB"]
	if !ok {
		defaultDb = restClient.params["DB"]
	}
	return &Conn{restClient: restClient, defaultDb: defaultDb}, nil
}
