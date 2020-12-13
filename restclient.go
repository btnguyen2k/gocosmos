package go_cosmos

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/gjrc"
)

// NewRestClient constructs a new RestClient instance from the supplied connection string.
//
// - if httpClient is supplied, reuse it. Otherwise, a new http.Client instance is created.
// - connStr is expected to be in the following format:
// AccountEndpoint=<cosmosdb-restapi-endpoint>;AccountKey=<account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>]
// If not supplied, default value for TimeoutMs is 10 seconds and Version is "2018-12-31".
func NewRestClient(httpClient *http.Client, connStr string) (*RestClient, error) {
	params := make(map[string]string)
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		tokens := strings.SplitN(part, "=", 2)
		if len(tokens) == 2 {
			params[strings.ToUpper(tokens[0])] = strings.TrimSpace(tokens[1])
		} else {
			params[strings.ToUpper(tokens[0])] = ""
		}
	}
	endpoint := strings.TrimSuffix(params["ACCOUNTENDPOINT"], "/")
	if endpoint == "" {
		return nil, errors.New("AccountEndpoint not found in connection string")
	}
	accountKey := params["ACCOUNTKEY"]
	if accountKey == "" {
		return nil, errors.New("AccountKey not found in connection string")
	}
	key, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return nil, fmt.Errorf("cannot base64 decode account key: %s", err)
	}
	timeoutMs, err := strconv.Atoi(params["TIMEOUTMS"])
	if err != nil || timeoutMs < 0 {
		timeoutMs = 10000
	}
	apiVersion := params["VERSION"]
	if apiVersion == "" {
		apiVersion = "2018-12-31"
	}
	return &RestClient{
		client:     gjrc.NewGjrc(httpClient, time.Duration(timeoutMs)*time.Millisecond),
		endpoint:   endpoint,
		authKey:    key,
		apiVersion: apiVersion,
	}, nil
}

// RestClient is REST-based client for Azure CosmosDB
type RestClient struct {
	client     *gjrc.Gjrc
	endpoint   string // Azure CosmosDB endpoint
	authKey    []byte // Account key to authenticate
	apiVersion string // Azure CosmosDB API version
}

func (c *RestClient) buildJsonRequest(method, url string, params interface{}) *http.Request {
	var r *bytes.Reader
	if params != nil {
		js, _ := json.Marshal(params)
		r = bytes.NewReader(js)
	} else {
		r = bytes.NewReader([]byte{})
	}
	req, _ := http.NewRequest(method, url, r)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-ms-version", c.apiVersion)
	return req
}

func (c *RestClient) addAuthHeader(req *http.Request, method, resType, resId string) *http.Request {
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

func (c *RestClient) buildRestReponse(resp *gjrc.GjrcResponse) RestReponse {
	result := RestReponse{CallErr: resp.Error()}
	if result.CallErr == nil {
		result.StatusCode = resp.StatusCode()
		result.RespBody, _ = resp.Body()
		result.RespHeader = make(map[string]string)
		for k, v := range resp.HttpResponse().Header {
			if len(v) > 0 {
				result.RespHeader[k] = v[0]
			}
		}
		if v, err := strconv.ParseFloat(result.RespHeader["x-ms-request-charge"], 64); err != nil {
			result.RequestCharge = v
		} else {
			result.RequestCharge = -1
		}
		result.SessionToken = result.RespHeader["x-ms-session-token"]
		if result.StatusCode >= 400 {
			result.ApiErr = fmt.Errorf("error executing Azure CosmosDB command; StatusCode=%d;Body=%s", result.StatusCode, result.RespBody)
		}
	}
	return result
}

// DatabaseSpec specifies a CosmosDB database specifications for creation.
type DatabaseSpec struct {
	Id        string
	Ru, MaxRu int
}

// CreateDatabase invokes CosmosDB API to create a new database.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-database
// Note: ru and maxru must not be supplied together!
func (c *RestClient) CreateDatabase(spec DatabaseSpec) *RespCreateDb {
	method := "POST"
	url := c.endpoint + "/dbs"
	req := c.buildJsonRequest(method, url, map[string]interface{}{"id": spec.Id})
	req = c.addAuthHeader(req, method, "dbs", "")
	if spec.Ru > 0 {
		req.Header.Set("x-ms-offer-throughput", strconv.Itoa(spec.Ru))
	}
	if spec.MaxRu > 0 {
		req.Header.Set("x-ms-cosmos-offer-autopilot-settings", fmt.Sprintf(`{"maxThroughput":%d}`, spec.MaxRu))
	}

	resp := c.client.Do(req)
	result := &RespCreateDb{RestReponse: c.buildRestReponse(resp), DbInfo: DbInfo{Id: spec.Id}}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DbInfo))
	}
	return result
}

// GetDatabase invokes CosmosDB API to get an existing database.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-a-database
func (c *RestClient) GetDatabase(dbName string) *RespGetDb {
	method := "GET"
	url := c.endpoint + "/dbs/" + dbName
	req := c.buildJsonRequest(method, url, nil)
	req = c.addAuthHeader(req, method, "dbs", "dbs/"+dbName)

	resp := c.client.Do(req)
	result := &RespGetDb{RestReponse: c.buildRestReponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DbInfo))
	}
	return result
}

// DeleteDatabase invokes CosmosDB API to delete an existing database.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/delete-a-database
func (c *RestClient) DeleteDatabase(dbName string) *RespDeleteDb {
	method := "DELETE"
	url := c.endpoint + "/dbs/" + dbName
	req := c.buildJsonRequest(method, url, nil)
	req = c.addAuthHeader(req, method, "dbs", "dbs/"+dbName)

	resp := c.client.Do(req)
	result := &RespDeleteDb{RestReponse: c.buildRestReponse(resp)}
	return result
}

// ListDatabases invokes CosmosDB API to list all available databases.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/list-databases
func (c *RestClient) ListDatabases() *RespListDb {
	method := "GET"
	url := c.endpoint + "/dbs"
	req := c.buildJsonRequest(method, url, nil)
	req = c.addAuthHeader(req, method, "dbs", "")

	resp := c.client.Do(req)
	result := &RespListDb{RestReponse: c.buildRestReponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &result)
		if result.CallErr == nil {
			sort.Slice(result.Databases, func(i, j int) bool {
				// sort databases by id
				return result.Databases[i].Id < result.Databases[j].Id
			})
		}
	}
	return result
}

// CollectionSpec specifies a CosmosDB collection specifications for creation.
type CollectionSpec struct {
	DbName, CollName string
	Ru, MaxRu        int
	// PartitionKeyInfo specifies the collection's partition key.
	// At the minimum, the partition key info is a map: {paths:[/path],"kind":"Hash"}
	// If partition key is larger than 100 bytes, specify {"Version":2}
	PartitionKeyInfo map[string]interface{}
	IndexingPolicy   map[string]interface{}
	UniqueKeyPolicy  map[string]interface{}
}

// CreateCollection invokes CosmosDB API to create a new collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
// Note: ru and maxru must not be supplied together!
func (c *RestClient) CreateCollection(spec CollectionSpec) *RespCreateColl {
	method := "POST"
	url := c.endpoint + "/dbs/" + spec.DbName + "/colls"
	params := map[string]interface{}{"id": spec.CollName, "partitionKey": spec.PartitionKeyInfo}
	if spec.IndexingPolicy != nil {
		params["indexingPolicy"] = spec.IndexingPolicy
	}
	if spec.UniqueKeyPolicy != nil {
		params["uniqueKeyPolicy"] = spec.UniqueKeyPolicy
	}
	req := c.buildJsonRequest(method, url, params)
	req = c.addAuthHeader(req, method, "colls", "dbs/"+spec.DbName)
	if spec.Ru > 0 {
		req.Header.Set("x-ms-offer-throughput", strconv.Itoa(spec.Ru))
	}
	if spec.MaxRu > 0 {
		req.Header.Set("x-ms-cosmos-offer-autopilot-settings", fmt.Sprintf(`{"maxThroughput":%d}`, spec.MaxRu))
	}

	resp := c.client.Do(req)
	result := &RespCreateColl{RestReponse: c.buildRestReponse(resp), CollInfo: CollInfo{Id: spec.CollName}}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.CollInfo))
	}
	return result
}

// ReplaceCollection invokes CosmosDB API to replace an existing collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/replace-a-collection
// Note: ru and maxru must not be supplied together!
func (c *RestClient) ReplaceCollection(spec CollectionSpec) *RespReplaceColl {
	method := "PUT"
	url := c.endpoint + "/dbs/" + spec.DbName + "/colls/" + spec.CollName
	params := map[string]interface{}{"id": spec.CollName}
	if spec.PartitionKeyInfo != nil {
		params["partitionKey"] = spec.PartitionKeyInfo
	}
	if spec.IndexingPolicy != nil {
		params["indexingPolicy"] = spec.IndexingPolicy
	}
	if spec.UniqueKeyPolicy != nil {
		params["uniqueKeyPolicy"] = spec.UniqueKeyPolicy
	}
	req := c.buildJsonRequest(method, url, params)
	req = c.addAuthHeader(req, method, "colls", "dbs/"+spec.DbName+"/colls/"+spec.CollName)
	if spec.Ru > 0 {
		req.Header.Set("x-ms-offer-throughput", strconv.Itoa(spec.Ru))
	}
	if spec.MaxRu > 0 {
		req.Header.Set("x-ms-cosmos-offer-autopilot-settings", fmt.Sprintf(`{"maxThroughput":%d}`, spec.MaxRu))
	}

	resp := c.client.Do(req)
	result := &RespReplaceColl{RestReponse: c.buildRestReponse(resp), CollInfo: CollInfo{Id: spec.CollName}}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.CollInfo))
	}
	return result
}

// GetCollection invokes CosmosDB API to get an existing collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-a-collection
func (c *RestClient) GetCollection(dbName, collName string) *RespGetColl {
	method := "GET"
	url := c.endpoint + "/dbs/" + dbName + "/colls/" + collName
	req := c.buildJsonRequest(method, url, nil)
	req = c.addAuthHeader(req, method, "colls", "dbs/"+dbName+"/colls/"+collName)

	resp := c.client.Do(req)
	result := &RespGetColl{RestReponse: c.buildRestReponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.CollInfo))
	}
	return result
}

// DeleteCollection invokes CosmosDB API to delete an existing collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/delete-a-collection
func (c *RestClient) DeleteCollection(dbName, collName string) *RespDeleteColl {
	method := "DELETE"
	url := c.endpoint + "/dbs/" + dbName + "/colls/" + collName
	req := c.buildJsonRequest(method, url, nil)
	req = c.addAuthHeader(req, method, "colls", "dbs/"+dbName+"/colls/"+collName)

	resp := c.client.Do(req)
	result := &RespDeleteColl{RestReponse: c.buildRestReponse(resp)}
	return result
}

// ListCollections invokes CosmosDB API to list all available collections.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/list-collections
func (c *RestClient) ListCollections(dbName string) *RespListColl {
	method := "GET"
	url := c.endpoint + "/dbs/" + dbName + "/colls"
	req := c.buildJsonRequest(method, url, nil)
	req = c.addAuthHeader(req, method, "colls", "dbs/"+dbName)

	resp := c.client.Do(req)
	result := &RespListColl{RestReponse: c.buildRestReponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &result)
		if result.CallErr == nil {
			sort.Slice(result.Collections, func(i, j int) bool {
				// sort collections by id
				return result.Collections[i].Id < result.Collections[j].Id
			})
		}
	}
	return result
}

// DocumentSpec specifies a CosmosDB document specifications for creation.
type DocumentSpec struct {
	DbName, CollName   string
	IsUpsert           bool
	IndexingDirective  string // accepted value "", "Include" or "Exclude"
	PartitionKeyValues []interface{}
	DocumentData       map[string]interface{}
}

// CreateDocument invokes CosmosDB API to create a new document.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-document
func (c *RestClient) CreateDocument(spec DocumentSpec) *RespCreateDoc {
	method := "POST"
	url := c.endpoint + "/dbs/" + spec.DbName + "/colls/" + spec.CollName + "/docs"
	req := c.buildJsonRequest(method, url, spec.DocumentData)
	req = c.addAuthHeader(req, method, "docs", "dbs/"+spec.DbName+"/colls/"+spec.CollName)
	if spec.IsUpsert {
		req.Header.Set("x-ms-documentdb-is-upsert", "true")
	}
	if spec.IndexingDirective != "" {
		req.Header.Set("x-ms-indexing-directive", spec.IndexingDirective)
	}
	jsPkValues, _ := json.Marshal(spec.PartitionKeyValues)
	req.Header.Set("x-ms-documentdb-partitionkey", string(jsPkValues))

	resp := c.client.Do(req)
	result := &RespCreateDoc{RestReponse: c.buildRestReponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DocInfo))
	}
	return result
}

/*----------------------------------------------------------------------*/

// RestReponse captures the response from REST API call.
type RestReponse struct {
	// CallErr holds any error occurred during the REST call.
	CallErr error
	// ApiErr holds any error occurred during the API call (only available when StatusCode >= 400).
	ApiErr error
	// StatusCode captures the HTTP status code from the REST call.
	StatusCode int
	// RespBody captures the body response from the REST call.
	RespBody []byte
	// RespHeader captures the header response from the REST call.
	RespHeader map[string]string
	// RequestCharge is number of request units consumed by the operation
	RequestCharge float64
	// SessionToken is used with session level consistency. Clients must save this value and set it for subsequent read requests for session consistency.
	SessionToken string
}

// Error returns CallErr if not nil, ApiErr otherwise.
func (r RestReponse) Error() error {
	if r.CallErr != nil {
		return r.CallErr
	}
	return r.ApiErr
}

// DbInfo captures info of a CosmosDB database.
type DbInfo struct {
	Id    string `json:"id"`     // user-generated unique name for the database
	Rid   string `json:"_rid"`   // (system generated property) _rid attribute of the database
	Ts    int64  `json:"_ts"`    // (system-generated property) _ts attribute of the database
	Self  string `json:"_self"`  // (system-generated property) _self attribute of the database
	Etag  string `json:"_etag"`  // (system-generated property) _etag attribute of the database
	Colls string `json:"_colls"` // (system-generated property) _colls attribute of the database
	Users string `json:"_users"` // (system-generated property) _users attribute of the database
}

// RespCreateDb captures the response from CreateDatabase call.
type RespCreateDb struct {
	RestReponse
	DbInfo
}

// RespGetDb captures the response from GetDatabase call.
type RespGetDb struct {
	RestReponse
	DbInfo
}

// RespDeleteDb captures the response from DeleteDatabase call.
type RespDeleteDb struct {
	RestReponse
}

// RespListDb captures the response from ListDatabases call.
type RespListDb struct {
	RestReponse `json:"-"`
	Count       int64    `json:"_count"` // number of databases returned from the list operation
	Databases   []DbInfo `json:"Databases"`
}

// CollInfo captures info of a CosmosDB collection.
type CollInfo struct {
	Id                       string                 `json:"id"`                       // user-generated unique name for the collection
	Rid                      string                 `json:"_rid"`                     // (system generated property) _rid attribute of the collection
	Ts                       int64                  `json:"_ts"`                      // (system-generated property) _ts attribute of the collection
	Self                     string                 `json:"_self"`                    // (system-generated property) _self attribute of the collection
	Etag                     string                 `json:"_etag"`                    // (system-generated property) _etag attribute of the collection
	Docs                     string                 `json:"_docs"`                    // (system-generated property) _docs attribute of the collection
	Sprocs                   string                 `json:"_sprocs"`                  // (system-generated property) _sprocs attribute of the collection
	Triggers                 string                 `json:"_triggers"`                // (system-generated property) _triggers attribute of the collection
	Udfs                     string                 `json:"_udfs"`                    // (system-generated property) _udfs attribute of the collection
	Conflicts                string                 `json:"_conflicts"`               // (system-generated property) _conflicts attribute of the collection
	IndexingPolicy           map[string]interface{} `json:"indexingPolicy"`           // indexing policy settings for collection
	PartitionKey             map[string]interface{} `json:"partitionKey"`             // partitioning configuration settings for collection
	ConflictResolutionPolicy map[string]interface{} `json:"conflictResolutionPolicy"` // conflict resolution policy settings for collection
	GeospatialConfig         map[string]interface{} `json:"geospatialConfig"`         // Geo-spatial configuration settings for collection
}

// RespCreateColl captures the response from CreateCollection call.
type RespCreateColl struct {
	RestReponse
	CollInfo
}

// RespReplaceColl captures the response from ReplaceCollection call.
type RespReplaceColl struct {
	RestReponse
	CollInfo
}

// RespGetColl captures the response from GetCollection call.
type RespGetColl struct {
	RestReponse
	CollInfo
}

// RespDeleteColl captures the response from DeleteCollection call.
type RespDeleteColl struct {
	RestReponse
}

// RespListColl captures the response from ListCollections call.
type RespListColl struct {
	RestReponse `json:"-"`
	Count       int64      `json:"_count"` // number of collections returned from the list operation
	Collections []CollInfo `json:"DocumentCollections"`
}

// DocInfo captures info of a CosmosDB document.
type DocInfo map[string]interface{}

// RespCreateDoc captures the response from CreateDocument call.
type RespCreateDoc struct {
	RestReponse
	DocInfo
}

// RespGetDoc captures the response from GetDocument call.
type RespGetDoc struct {
	RestReponse
	DocInfo
}

// RespDeleteDoc captures the response from DeleteDocument call.
type RespDeleteDoc struct {
	RestReponse
}