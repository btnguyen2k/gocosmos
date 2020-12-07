package go_cosmos

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/gjrc"
)

var (
	locGmt, _ = time.LoadLocation("GMT")
)

// Conn is Azure CosmosDB connection handle.
type Conn struct {
	client   *gjrc.Gjrc
	endpoint string            // Azure CosmosDB endpoint
	authKey  []byte            // Account key to authenticate
	params   map[string]string // other parameters
}

func (c *Conn) buildJsonRequest(method, url string, params interface{}) *http.Request {
	var r *bytes.Reader
	if params != nil {
		js, _ := json.Marshal(params)
		r = bytes.NewReader(js)
	} else {
		r = bytes.NewReader([]byte{})
	}
	req, _ := http.NewRequest(method, url, r)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-ms-version", "2018-12-31")
	return req
}

func (c *Conn) addAuthHeader(req *http.Request, method, resType, resId string) *http.Request {
	now := time.Now().In(locGmt)
	stringToSign := strings.ToLower(fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n", method, resType, resId, now.Format(time.RFC1123), ""))
	h := hmac.New(sha256.New, c.authKey)
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	authHeader := "type=master&ver=1.0&sig=" + signature
	authHeader = url.QueryEscape(authHeader)
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("x-ms-date", now.Format(time.RFC1123))
	return req
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
