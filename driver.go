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
var lock = &sync.Mutex{}
var httpClientSingleton *http.Client

func getHttpClientInstance() *http.Client {
	if httpClientSingleton == nil {
		lock.Lock()
		defer lock.Unlock()
		if httpClientSingleton == nil {
			httpClientSingleton = httpClientFactory()
		}
	}
	return httpClientSingleton
}
func httpClientFactory() *http.Client {
	const timeoutMs = 10000
	const insecureSkipVerify = true
	return &http.Client{
		Timeout:   time.Duration(timeoutMs) * time.Millisecond,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify}},
	}
}

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
)

// Driver is Azure CosmosDB driver for database/sql.
type Driver struct {
}

// Open implements driver.Driver.Open.
//
// connStr is expected in the following format:
//     AccountEndpoint=<cosmosdb-restapi-endpoint>;AccountKey=<account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>][;DefaultDb=<db-name>][;AutoId=<true/false>]
//
// If not supplied, default value for TimeoutMs is 10 seconds, Version is "2018-12-31" and AutoId is true.
//
// DefaultDb is added since v0.1.1
//
// AutoId is added since v0.1.2
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	restClient, err := NewRestClient(getHttpClientInstance(), connStr)
	if err != nil {
		return nil, err
	}
	defaultDb, ok := restClient.params["DEFAULTDB"]
	if !ok {
		defaultDb, _ = restClient.params["DB"]
	}
	return &Conn{restClient: restClient, defaultDb: defaultDb}, nil
}
