package gocosmos

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/gjrc"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
)

const (
	settingEndpoint           = "ACCOUNTENDPOINT"
	settingAccountKey         = "ACCOUNTKEY"
	settingTimeout            = "TIMEOUTMS"
	settingVersion            = "VERSION"
	settingAutoId             = "AUTOID"
	settingInsecureSkipVerify = "INSECURESKIPVERIFY"

	// DefaultApiVersion holds the default REST API version if not specified in the connection string.
	//
	// See: https://learn.microsoft.com/en-us/rest/api/cosmos-db/#supported-rest-api-versions
	//
	// @Available since v0.3.0
	DefaultApiVersion = "2020-07-15"
)

// NewRestClient constructs a new RestClient instance from the supplied connection string.
//
// httpClient is reused if supplied. Otherwise, a new http.Client instance is created.
// connStr is expected to be in the following format:
//
//	AccountEndpoint=<cosmosdb-restapi-endpoint>;AccountKey=<account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>][;AutoId=<true/false>][;InsecureSkipVerify=<true/false>]
//
// If not supplied, default value for TimeoutMs is 10 seconds, Version is DefaultApiVersion (which is "2020-07-15"), AutoId is true, and InsecureSkipVerify is false
//
// - AutoId is added since v0.1.2
// - InsecureSkipVerify is added since v0.1.4
func NewRestClient(httpClient *http.Client, connStr string) (*RestClient, error) {
	params := make(map[string]string)
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		tokens := strings.SplitN(part, "=", 2)
		key := strings.ToUpper(strings.TrimSpace(tokens[0]))
		if len(tokens) == 2 {
			params[key] = strings.TrimSpace(tokens[1])
		} else {
			params[key] = ""
		}
	}
	endpoint := strings.TrimSuffix(params[settingEndpoint], "/")
	if endpoint == "" {
		return nil, errors.New("AccountEndpoint not found in connection string")
	}
	accountKey := params[settingAccountKey]
	if accountKey == "" {
		return nil, errors.New("AccountKey not found in connection string")
	}
	key, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return nil, fmt.Errorf("cannot base64 decode account key: %s", err)
	}
	timeoutMs, err := strconv.Atoi(params[settingTimeout])
	if err != nil || timeoutMs < 0 {
		timeoutMs = 10000
	}
	apiVersion := params[settingVersion]
	if apiVersion == "" {
		apiVersion = DefaultApiVersion
	}
	autoId, err := strconv.ParseBool(params[settingAutoId])
	if err != nil {
		autoId = true
	}
	insecureSkipVerify, err := strconv.ParseBool(params[settingInsecureSkipVerify])
	if err != nil {
		insecureSkipVerify = false
	}
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout:   time.Duration(timeoutMs) * time.Millisecond,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify}},
		}
	}
	return &RestClient{
		client:     gjrc.NewGjrc(httpClient, time.Duration(timeoutMs)*time.Millisecond),
		endpoint:   endpoint,
		authKey:    key,
		apiVersion: apiVersion,
		autoId:     autoId,
		params:     params,
	}, nil
}

// RestClient is REST-based client for Azure Cosmos DB
type RestClient struct {
	client     *gjrc.Gjrc
	endpoint   string            // Azure Cosmos DB endpoint
	authKey    []byte            // Account key to authenticate
	apiVersion string            // Azure Cosmos DB API version
	autoId     bool              // if true and value for 'id' field is not specified, CreateDocument
	params     map[string]string // parsed parameters
}

func (c *RestClient) buildJsonRequest(method, url string, params interface{}) (*http.Request, error) {
	var r *bytes.Reader
	if params != nil {
		js, _ := json.Marshal(params)
		r = bytes.NewReader(js)
	} else {
		r = bytes.NewReader([]byte{})
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set(httpHeaderContentType, "application/json")
	req.Header.Set(httpHeaderAccept, "application/json")
	req.Header.Set(restApiHeaderVersion, c.apiVersion)
	return req, nil
}

func (c *RestClient) addAuthHeader(req *http.Request, method, resType, resId string) *http.Request {
	now := time.Now().In(locGmt)
	/*
	 * M.A.I. 2022-02-16
	 * The original statement had a single ToLower. In the resulting string the resId gets lowered when from MS Docs it should be left unaltered
	 * I came across an error on a collection with a mixed case name...
	 * stringToSign := strings.ToLower(fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n", method, resType, resId, now.Format(time.RFC1123), ""))
	 */
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n", strings.ToLower(method), strings.ToLower(resType), resId, strings.ToLower(now.Format(time.RFC1123)), "")
	h := hmac.New(sha256.New, c.authKey)
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	authHeader := "type=master&ver=1.0&sig=" + signature
	authHeader = url.QueryEscape(authHeader)
	req.Header.Set(httpHeaderAuthorization, authHeader)
	req.Header.Set(restApiHeaderDate, now.Format(time.RFC1123))
	return req
}

func (c *RestClient) buildRestResponse(resp *gjrc.GjrcResponse) RestResponse {
	result := RestResponse{CallErr: resp.Error()}
	if result.CallErr != nil {
		httpResp := resp.HttpResponse()
		if httpResp != nil {
			result.StatusCode = resp.StatusCode()
			if result.StatusCode == 204 || result.StatusCode == 304 {
				//Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
				//The DELETE operation is successful, no content is returned.

				//Server may return status "304 Not Modified", no content is returned.

				result.CallErr = nil
			} else {
				result.RespBody, _ = resp.Body()
			}
		}
		if result.CallErr != nil {
			result.CallErr = fmt.Errorf("status-code: %d / error: %s / response-body: %s", result.StatusCode, result.CallErr, result.RespBody)
		}
	}
	if result.CallErr == nil {
		result.StatusCode = resp.StatusCode()
		result.RespBody, _ = resp.Body()
		result.RespHeader = make(map[string]string)
		for k, v := range resp.HttpResponse().Header {
			if len(v) > 0 {
				result.RespHeader[strings.ToUpper(k)] = v[0]
			}
		}
		if v, err := strconv.ParseFloat(result.RespHeader[respHeaderRequestCharge], 64); err == nil {
			result.RequestCharge = v
		} else {
			result.RequestCharge = -1
		}
		result.SessionToken = result.RespHeader[respHeaderSessionToken]
		if result.StatusCode >= 400 {
			result.ApiErr = fmt.Errorf("error executing Azure Cosmos DB command; StatusCode=%d;Body=%s", result.StatusCode, result.RespBody)
		}
	}
	return result
}

// GetApiVersion returns the Azure Cosmos DB APi version string, either from connection string or default value.
//
// @Available since v1.0.0
func (c *RestClient) GetApiVersion() string {
	return c.apiVersion
}

// GetAutoId returns the auto-id flag.
//
// @Available since v1.0.0
func (c *RestClient) GetAutoId() bool {
	return c.autoId
}

// SetAutoId sets value for the auto-id flag.
//
// @Available since v1.0.0
func (c *RestClient) SetAutoId(value bool) *RestClient {
	c.autoId = value
	return c
}

/*----------------------------------------------------------------------*/

// DatabaseSpec specifies a Cosmos DB database specifications for creation.
type DatabaseSpec struct {
	Id        string
	Ru, MaxRu int
}

// CreateDatabase invokes Cosmos DB API to create a new database.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-database.
//
// Note: ru and maxru must not be supplied together!
func (c *RestClient) CreateDatabase(spec DatabaseSpec) *RespCreateDb {
	method, urlEndpoint := "POST", c.endpoint+"/dbs"
	req, err := c.buildJsonRequest(method, urlEndpoint, map[string]interface{}{"id": spec.Id})
	if err != nil {
		return &RespCreateDb{RestResponse: RestResponse{CallErr: err}, DbInfo: DbInfo{Id: spec.Id}}
	}
	req = c.addAuthHeader(req, method, "dbs", "")
	if spec.Ru > 0 {
		req.Header.Set(restApiHeaderOfferThroughput, strconv.Itoa(spec.Ru))
	}
	if spec.MaxRu > 0 {
		req.Header.Set(restApiHeaderOfferAutopilotSettings, fmt.Sprintf(`{"maxThroughput":%d}`, spec.MaxRu))
	}

	resp := c.client.Do(req)
	result := &RespCreateDb{RestResponse: c.buildRestResponse(resp), DbInfo: DbInfo{Id: spec.Id}}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DbInfo))
	}
	return result
}

// GetDatabase invokes Cosmos DB API to get an existing database.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-a-database.
func (c *RestClient) GetDatabase(dbName string) *RespGetDb {
	method, urlEndpoint := "GET", c.endpoint+"/dbs/"+dbName
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespGetDb{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "dbs", "dbs/"+dbName)

	resp := c.client.Do(req)
	result := &RespGetDb{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DbInfo))
	}
	return result
}

// DeleteDatabase invokes Cosmos DB API to delete an existing database.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/delete-a-database.
func (c *RestClient) DeleteDatabase(dbName string) *RespDeleteDb {
	method, urlEndpoint := "DELETE", c.endpoint+"/dbs/"+dbName
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespDeleteDb{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "dbs", "dbs/"+dbName)

	resp := c.client.Do(req)
	result := &RespDeleteDb{RestResponse: c.buildRestResponse(resp)}
	return result
}

// ListDatabases invokes Cosmos DB API to list all available databases.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/list-databases.
func (c *RestClient) ListDatabases() *RespListDb {
	method, urlEndpoint := "GET", c.endpoint+"/dbs"
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespListDb{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "dbs", "")

	resp := c.client.Do(req)
	result := &RespListDb{RestResponse: c.buildRestResponse(resp)}
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

/*----------------------------------------------------------------------*/

// CollectionSpec specifies a Cosmos DB collection specifications for creation.
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

// CreateCollection invokes Cosmos DB API to create a new collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection.
//
// Note: ru and maxru must not be supplied together!
func (c *RestClient) CreateCollection(spec CollectionSpec) *RespCreateColl {
	method, urlEndpoint := "POST", c.endpoint+"/dbs/"+spec.DbName+"/colls"
	params := map[string]interface{}{"id": spec.CollName, "partitionKey": spec.PartitionKeyInfo}
	if spec.IndexingPolicy != nil {
		params[restApiParamIndexingPolicy] = spec.IndexingPolicy
	}
	if spec.UniqueKeyPolicy != nil {
		params[restApiParamUniqueKeyPolicy] = spec.UniqueKeyPolicy
	}
	req, err := c.buildJsonRequest(method, urlEndpoint, params)
	if err != nil {
		return &RespCreateColl{RestResponse: RestResponse{CallErr: err}, CollInfo: CollInfo{Id: spec.CollName}}
	}
	req = c.addAuthHeader(req, method, "colls", "dbs/"+spec.DbName)
	if spec.Ru > 0 {
		req.Header.Set(restApiHeaderOfferThroughput, strconv.Itoa(spec.Ru))
	}
	if spec.MaxRu > 0 {
		req.Header.Set(restApiHeaderOfferAutopilotSettings, fmt.Sprintf(`{"maxThroughput":%d}`, spec.MaxRu))
	}

	resp := c.client.Do(req)
	result := &RespCreateColl{RestResponse: c.buildRestResponse(resp), CollInfo: CollInfo{Id: spec.CollName}}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.CollInfo))
	}
	return result
}

// ReplaceCollection invokes Cosmos DB API to replace an existing collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/replace-a-collection.
//
// Note: ru and maxru must not be supplied together!
func (c *RestClient) ReplaceCollection(spec CollectionSpec) *RespReplaceColl {
	method, urlEndpoint := "PUT", c.endpoint+"/dbs/"+spec.DbName+"/colls/"+spec.CollName
	params := map[string]interface{}{"id": spec.CollName}
	if spec.PartitionKeyInfo != nil {
		params[restApiParamPartitionKey] = spec.PartitionKeyInfo
	}
	if spec.IndexingPolicy != nil {
		params[restApiParamIndexingPolicy] = spec.IndexingPolicy
	}
	// The unique index cannot be modified. To change the unique index, remove the collection and re-create a new one.
	// if spec.UniqueKeyPolicy != nil {
	// 	params[restApiParamUniqueKeyPolicy] = spec.UniqueKeyPolicy
	// }
	req, err := c.buildJsonRequest(method, urlEndpoint, params)
	if err != nil {
		return &RespReplaceColl{RestResponse: RestResponse{CallErr: err}, CollInfo: CollInfo{Id: spec.CollName}}
	}
	req = c.addAuthHeader(req, method, "colls", "dbs/"+spec.DbName+"/colls/"+spec.CollName)
	if spec.Ru > 0 {
		req.Header.Set(restApiHeaderOfferThroughput, strconv.Itoa(spec.Ru))
	}
	if spec.MaxRu > 0 {
		req.Header.Set(restApiHeaderOfferAutopilotSettings, fmt.Sprintf(`{"maxThroughput":%d}`, spec.MaxRu))
	}

	resp := c.client.Do(req)
	result := &RespReplaceColl{RestResponse: c.buildRestResponse(resp), CollInfo: CollInfo{Id: spec.CollName}}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.CollInfo))
	}
	return result
}

// GetCollection invokes Cosmos DB API to get an existing collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-a-collection
func (c *RestClient) GetCollection(dbName, collName string) *RespGetColl {
	method, urlEndpoint := "GET", c.endpoint+"/dbs/"+dbName+"/colls/"+collName
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespGetColl{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "colls", "dbs/"+dbName+"/colls/"+collName)

	resp := c.client.Do(req)
	result := &RespGetColl{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.CollInfo))
	}
	return result
}

// DeleteCollection invokes Cosmos DB API to delete an existing collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/delete-a-collection.
func (c *RestClient) DeleteCollection(dbName, collName string) *RespDeleteColl {
	method, urlEndpoint := "DELETE", c.endpoint+"/dbs/"+dbName+"/colls/"+collName
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespDeleteColl{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "colls", "dbs/"+dbName+"/colls/"+collName)

	resp := c.client.Do(req)
	result := &RespDeleteColl{RestResponse: c.buildRestResponse(resp)}
	return result
}

// ListCollections invokes Cosmos DB API to list all available collections.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/list-collections.
func (c *RestClient) ListCollections(dbName string) *RespListColl {
	method, urlEndpoint := "GET", c.endpoint+"/dbs/"+dbName+"/colls"
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespListColl{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "colls", "dbs/"+dbName)

	resp := c.client.Do(req)
	result := &RespListColl{RestResponse: c.buildRestResponse(resp)}
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

// GetPkranges invokes Cosmos DB API to retrieves the list of partition key ranges for a collection.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-partition-key-ranges.
//
// Available since v0.1.3
func (c *RestClient) GetPkranges(dbName, collName string) *RespGetPkranges {
	method, urlEndpoint := "GET", c.endpoint+"/dbs/"+dbName+"/colls/"+collName+"/pkranges"
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespGetPkranges{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "pkranges", "dbs/"+dbName+"/colls/"+collName)

	resp := c.client.Do(req)
	result := &RespGetPkranges{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &result)
	}
	return result
}

/*----------------------------------------------------------------------*/

// DocumentSpec specifies a Cosmos DB document specifications for creation.
type DocumentSpec struct {
	DbName, CollName   string
	IsUpsert           bool
	IndexingDirective  string // accepted value "", "Include" or "Exclude"
	PartitionKeyValues []interface{}
	DocumentData       DocInfo
}

// CreateDocument invokes Cosmos DB API to create a new document.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-document.
func (c *RestClient) CreateDocument(spec DocumentSpec) *RespCreateDoc {
	method, urlEndpoint := "POST", c.endpoint+"/dbs/"+spec.DbName+"/colls/"+spec.CollName+"/docs"
	if c.autoId {
		if id, ok := spec.DocumentData[docFieldId].(string); !ok || strings.TrimSpace(id) == "" {
			spec.DocumentData[docFieldId] = strings.ToLower(idGen.Id128Hex())
		}
	}
	req, err := c.buildJsonRequest(method, urlEndpoint, spec.DocumentData)
	if err != nil {
		return &RespCreateDoc{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+spec.DbName+"/colls/"+spec.CollName)
	if spec.IsUpsert {
		req.Header.Set(restApiHeaderIsUpsert, "true")
	}
	if spec.IndexingDirective != "" {
		req.Header.Set(restApiHeaderIndexingDirective, spec.IndexingDirective)
	}
	jsPkValues, _ := json.Marshal(spec.PartitionKeyValues)
	req.Header.Set(restApiHeaderPartitionKey, string(jsPkValues))

	resp := c.client.Do(req)
	result := &RespCreateDoc{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DocInfo))
	}
	return result
}

// ReplaceDocument invokes Cosmos DB API to replace an existing document.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/replace-a-document.
func (c *RestClient) ReplaceDocument(matchEtag string, spec DocumentSpec) *RespReplaceDoc {
	id, _ := spec.DocumentData[docFieldId].(string)
	method, urlEndpoint := "PUT", c.endpoint+"/dbs/"+spec.DbName+"/colls/"+spec.CollName+"/docs/"+id
	req, err := c.buildJsonRequest(method, urlEndpoint, spec.DocumentData)
	if err != nil {
		return &RespReplaceDoc{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+spec.DbName+"/colls/"+spec.CollName+"/docs/"+id)
	if matchEtag != "" {
		req.Header.Set(httpHeaderIfMatch, matchEtag)
	}
	jsPkValues, _ := json.Marshal(spec.PartitionKeyValues)
	req.Header.Set(restApiHeaderPartitionKey, string(jsPkValues))

	resp := c.client.Do(req)
	result := &RespReplaceDoc{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DocInfo))
	}
	return result
}

// DocReq specifies a document request.
type DocReq struct {
	DbName, CollName, DocId string
	PartitionKeyValues      []interface{}
	MatchEtag               string // if not empty, add "If-Match" header to request
	NotMatchEtag            string // if not empty, add "If-None-Match" header to request
	ConsistencyLevel        string // accepted values: "", "Strong", "Bounded", "Session" or "Eventual"
	SessionToken            string // string token used with session level consistency
}

// GetDocument invokes Cosmos DB API to get an existing document.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-a-document.
func (c *RestClient) GetDocument(r DocReq) *RespGetDoc {
	method, urlEndpoint := "GET", c.endpoint+"/dbs/"+r.DbName+"/colls/"+r.CollName+"/docs/"+r.DocId
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespGetDoc{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+r.DbName+"/colls/"+r.CollName+"/docs/"+r.DocId)
	jsPkValues, _ := json.Marshal(r.PartitionKeyValues)
	req.Header.Set(restApiHeaderPartitionKey, string(jsPkValues))
	if r.NotMatchEtag != "" {
		req.Header.Set(httpHeaderIfNoneMatch, r.NotMatchEtag)
	}
	if r.ConsistencyLevel != "" {
		req.Header.Set(restApiHeaderConsistencyLevel, r.ConsistencyLevel)
	}
	if r.SessionToken != "" {
		req.Header.Set(restApiHeaderSessionToken, r.SessionToken)
	}

	resp := c.client.Do(req)
	result := &RespGetDoc{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil && result.StatusCode != 304 {
		result.CallErr = json.Unmarshal(result.RespBody, &(result.DocInfo))
	}
	return result
}

// DeleteDocument invokes Cosmos DB API to delete an existing document.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/delete-a-document.
func (c *RestClient) DeleteDocument(r DocReq) *RespDeleteDoc {
	method, urlEndpoint := "DELETE", c.endpoint+"/dbs/"+r.DbName+"/colls/"+r.CollName+"/docs/"+r.DocId
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespDeleteDoc{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+r.DbName+"/colls/"+r.CollName+"/docs/"+r.DocId)
	jsPkValues, _ := json.Marshal(r.PartitionKeyValues)
	req.Header.Set(restApiHeaderPartitionKey, string(jsPkValues))
	if r.MatchEtag != "" {
		req.Header.Set(httpHeaderIfMatch, r.MatchEtag)
	}

	resp := c.client.Do(req)
	result := &RespDeleteDoc{RestResponse: c.buildRestResponse(resp)}
	return result
}

// QueryReq specifies a query request to query for documents.
type QueryReq struct {
	DbName, CollName      string
	Query                 string
	Params                []interface{}
	MaxItemCount          int    // if max-item-count = 0: use server side default value, (since v0.1.8) if max-item-count < 0: client will fetch all returned documents from server
	PkRangeId             string // (since v0.1.8) if non-empty, query will perform only on this PkRangeId (if PkRangeId and PkValue are specified, PkRangeId takes priority)
	PkValue               string // (since v0.1.8) if non-empty, query will perform only on the partition that PkValue maps to (if PkRangeId and PkValue are specified, PkRangeId takes priority)
	ContinuationToken     string
	CrossPartitionEnabled bool
	ConsistencyLevel      string // accepted values: "", "Strong", "Bounded", "Session" or "Eventual"
	SessionToken          string // string token used with session level consistency
}

func (c *RestClient) buildQueryRequest(query QueryReq) (*http.Request, error) {
	method, urlEndpoint := "POST", c.endpoint+"/dbs/"+query.DbName+"/colls/"+query.CollName+"/docs"
	requestBody := make(map[string]interface{}, 0)
	requestBody[restApiParamQuery] = query.Query
	if query.Params != nil {
		// M.A.I. 2022-02-16: server will complain if parameter set to nil
		requestBody[restApiParamParameters] = query.Params
	}
	req, err := c.buildJsonRequest(method, urlEndpoint, requestBody)
	if err != nil {
		return nil, err
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+query.DbName+"/colls/"+query.CollName)
	req.Header.Set(httpHeaderContentType, "application/query+json")
	req.Header.Set(restApiHeaderIsQuery, "true")
	req.Header.Set(restApiHeaderPopulateMetrics, "true")
	if query.MaxItemCount > 0 {
		req.Header.Set(restApiHeaderPageSize, strconv.Itoa(query.MaxItemCount))
	}
	if query.ContinuationToken != "" {
		req.Header.Set(restApiHeaderContinuation, query.ContinuationToken)
	}
	if query.ConsistencyLevel != "" {
		req.Header.Set(restApiHeaderConsistencyLevel, query.ConsistencyLevel)
	}
	if query.SessionToken != "" {
		req.Header.Set(restApiHeaderSessionToken, query.SessionToken)
	}
	if query.PkRangeId != "" {
		req.Header.Set(restApiHeaderPartitionKeyRangeId, query.PkRangeId)
	} else if query.PkValue != "" {
		req.Header.Set(restApiHeaderPartitionKey, `["`+query.PkValue+`"]`)
	}
	if query.CrossPartitionEnabled {
		req.Header.Set(restApiHeaderEnableCrossPartitionQuery, "true")
	}
	return req, nil
}

func (c *RestClient) mergeQueryResults(existingResp, newResp *RespQueryDocs, queryPlan *RespQueryPlan) *RespQueryDocs {
	temp := *newResp
	result := &temp
	if existingResp != nil {
		result.RequestCharge += existingResp.RequestCharge
		if newResp.Error() == nil {
			result = result.merge(queryPlan, existingResp)
		}
	} else if queryPlan.IsDistinctQuery() {
		result.Documents = result.Documents.ReduceDistinct(queryPlan)
		result.Count = len(result.Documents)
	}
	if queryPlan.QueryInfo.RewrittenQuery != "" {
		result.populateRewrittenDocuments(queryPlan)
	}
	return result
}

// Note: the query is executed as-is, not rewritten!
func (c *RestClient) queryAllAndMerge(query QueryReq, queryPlan *RespQueryPlan) *RespQueryDocs {
	var result *RespQueryDocs
	for {
		result = c.mergeQueryResults(result, c.queryDocumentsCall(query), queryPlan)
		if result.Error() != nil || result.ContinuationToken == "" || (query.MaxItemCount > 0 && result.Count >= query.MaxItemCount) {
			break
		}
		query.ContinuationToken = result.ContinuationToken
	}
	return result
}

// queryAndMerge queries documents then performs merging to build the final result.
//
// Note: query is rewritten, executed and flattened (transformed) before returned!
func (c *RestClient) queryAndMerge(query QueryReq, pkranges *RespGetPkranges, queryPlan *RespQueryPlan) *RespQueryDocs {
	queryRewritten := queryPlan.QueryInfo.RewrittenQuery != ""
	if queryRewritten {
		query.Query = strings.ReplaceAll(queryPlan.QueryInfo.RewrittenQuery, "{documentdb-formattableorderbyquery-filter}", "true")
	}

	var result *RespQueryDocs
	savedContinuationToken := query.ContinuationToken
	if query.PkValue != "" || query.PkRangeId != "" || pkranges.Count == 1 {
		if query.PkValue == "" && query.PkRangeId == "" {
			query.PkRangeId = pkranges.Pkranges[0].Id
		}
		result = c.queryDocumentsSimple(query, queryPlan)
	} else {
		var cctResult, cctQuery = make(map[string]string), make(map[string]string)
		if err := json.Unmarshal([]byte(query.ContinuationToken), &cctQuery); err != nil || query.ContinuationToken == "" {
			cctQuery = make(map[string]string)
			for _, pkrange := range pkranges.Pkranges {
				cctQuery[pkrange.Id] = ""
			}
		}
		for k, v := range cctQuery {
			cctResult[k] = v
		}

		savedMaxItemCount := query.MaxItemCount
		for _, pkrange := range pkranges.Pkranges {
			if continuationToken, ok := cctQuery[pkrange.Id]; !ok {
				// all documents from this pk-range had been queried
				continue
			} else {
				query.ContinuationToken = continuationToken
				query.PkRangeId = pkrange.Id
			}
			result = c.mergeQueryResults(result, c.queryAllAndMerge(query, queryPlan), queryPlan)
			if result.Error() != nil {
				break
			}
			if result.ContinuationToken == "" {
				delete(cctResult, pkrange.Id)
			} else {
				cctResult[pkrange.Id] = result.ContinuationToken
			}
			if len(cctResult) > 0 {
				js, _ := json.Marshal(cctResult)
				result.ContinuationToken = string(js)
			} else {
				result.ContinuationToken = ""
			}
			if query.MaxItemCount > 0 {
				// honor MaxItemCount setting
				if queryPlan.IsGroupByQuery() && result.Count > query.MaxItemCount {
					break
				}
				if result.Count >= query.MaxItemCount {
					break
				}
				query.MaxItemCount = savedMaxItemCount - result.Count
				if query.MaxItemCount <= 0 {
					break
				}
			}
			if queryPlan.QueryInfo.Limit > 0 && !queryPlan.IsGroupByQuery() && result.Count >= queryPlan.QueryInfo.Offset+queryPlan.QueryInfo.Limit {
				break
			}
		}
	}

	return c.finalPrepareResult(result, queryPlan, savedContinuationToken)
}

func (c *RestClient) finalPrepareResult(result *RespQueryDocs, queryPlan *RespQueryPlan, savedContinuationToken string) *RespQueryDocs {
	queryRewritten := queryPlan.QueryInfo.RewrittenQuery != ""
	if queryPlan.IsDistinctQuery() || queryPlan.IsGroupByQuery() {
		if queryPlan.IsDistinctQuery() {
			result.Documents = result.Documents.ReduceDistinct(queryPlan)
		} else {
			result.Documents = result.Documents.ReduceGroupBy(queryPlan)
		}
		result.Count = len(result.Documents)
		result.populateRewrittenDocuments(queryPlan)
	}

	if queryRewritten {
		result.Documents = result.Documents.Flatten(queryPlan)
		result.Count = len(result.Documents)
	}
	if queryPlan.QueryInfo.Limit > 0 {
		offset, limit := queryPlan.QueryInfo.Offset, queryPlan.QueryInfo.Limit
		if savedContinuationToken != "" && queryRewritten {
			offset = 0
		}
		if len(result.Documents)-offset < limit {
			limit = len(result.Documents) - offset
		}
		if limit > 0 {
			result.Documents = result.Documents[offset : offset+limit]
			if result.RewrittenDocuments != nil {
				result.RewrittenDocuments = result.RewrittenDocuments[offset : offset+limit]
			}
		} else {
			result.Documents = result.Documents[0:0]
			if result.RewrittenDocuments != nil {
				result.RewrittenDocuments = result.RewrittenDocuments[0:0]
			}
		}
		result.Count = len(result.Documents)
	}
	return result
}

// queryDocumentsSimple handle a query-documents request with simple SQL query.
//
// If QueryReq.MaxItemCount <= 0, all matched documents will be returned
//
// Note: query is executed as-is, not rewritten!
func (c *RestClient) queryDocumentsSimple(query QueryReq, queryPlan *RespQueryPlan) *RespQueryDocs {
	req, err := c.buildQueryRequest(query)
	if err != nil {
		return &RespQueryDocs{RestResponse: RestResponse{CallErr: err}}
	}
	var result *RespQueryDocs
	if query.MaxItemCount <= 0 {
		// request chunk by chunk as it would have negative impact if we fetch a large number of documents in one go
		req.Header.Set(restApiHeaderPageSize, "100")
	}
	for {
		resp := c.client.Do(req)
		tempResult := &RespQueryDocs{RestResponse: c.buildRestResponse(resp)}
		if tempResult.CallErr == nil {
			tempResult.ContinuationToken = tempResult.RespHeader[respHeaderContinuation]
			tempResult.CallErr = json.Unmarshal(tempResult.RespBody, &tempResult)
		}
		if result != nil {
			// append returned document list
			tempResult.Count += result.Count
			tempResult.RequestCharge += result.RequestCharge
			tempResult.Documents = append(result.Documents, tempResult.Documents...)
		}
		result = tempResult
		if result.Error() != nil || query.MaxItemCount > 0 || result.ContinuationToken == "" {
			break
		}
		req.Header.Set(restApiHeaderContinuation, tempResult.ContinuationToken)
	}
	return result.populateRewrittenDocuments(queryPlan)
}

// queryDocumentsCall makes a single query-documents API call.
//
// Note: the query is executed as-is!
func (c *RestClient) queryDocumentsCall(query QueryReq) *RespQueryDocs {
	req, err := c.buildQueryRequest(query)
	if err != nil {
		return &RespQueryDocs{RestResponse: RestResponse{CallErr: err}}
	}
	resp := c.client.Do(req)
	result := &RespQueryDocs{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.ContinuationToken = result.RespHeader[respHeaderContinuation]
		result.CallErr = json.Unmarshal(result.RespBody, &result)
	}
	return result
}

// QueryDocuments invokes Cosmos DB API to query a collection for documents.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/query-documents.
//
// Known issues:
//   - (*) `GROUP BY` with `ORDER BY` queries are currently not supported by Cosmos DB! Resolution/Workaround: NONE!
//   - Paging a cross-partition `OFFSET...LIMIT` query using QueryReq.MaxItemCount: it would not work. Moreover, the
//     result returned from QueryDocumentsCrossPartition might be different from or the one returned from call to
//     QueryDocuments without pagination. Resolution/Workaround: NONE!
//   - Paging a cross-partition `ORDER BY` query using QueryReq.MaxItemCount would not work: returned rows might not be
//     in the expected order. Resolution/Workaround: use QueryDocumentsCrossPartition or QueryDocuments without paging
//     (caution: intermediate results are kept in memory, be alerted for out-of-memory error).
//   - Paging a cross-partition `SELECT DISTINCT/VALUE` query using QueryReq.MaxItemCount would not work: returned rows
//     might be duplicated. Resolution/Workaround: use QueryDocumentsCrossPartition or QueryDocuments without paging
//     (caution: intermediate results are kept in memory, be alerted for out-of-memory error).
//   - Cross-partition queries that combine `GROUP BY` with QueryReq.MaxItemCount would not work: the aggregate function
//     might not work properly. Resolution/Workaround: use QueryDocumentsCrossPartition or QueryDocuments without
//     QueryReq.MaxItemCount (caution: intermediate results are kept in memory, be alerted for out-of-memory error).
func (c *RestClient) QueryDocuments(query QueryReq) *RespQueryDocs {
	queryPlan := c.QueryPlan(query)
	if queryPlan.Error() != nil {
		return &RespQueryDocs{RestResponse: queryPlan.RestResponse}
	}

	if queryPlan.QueryInfo.DistinctType != "None" || queryPlan.QueryInfo.RewrittenQuery != "" {
		pkranges := c.GetPkranges(query.DbName, query.CollName)
		if pkranges.Error() != nil {
			return &RespQueryDocs{RestResponse: pkranges.RestResponse}
		}
		return c.queryAndMerge(query, pkranges, queryPlan)
	}

	return c.queryDocumentsSimple(query, queryPlan)
}

// QueryDocumentsCrossPartition can be used as a workaround for known issues with QueryDocuments.
//
// Caution: intermediate results are kept in memory, and all matched rows are returned. Be alerted for out-of-memory error!
//
// Available since v0.2.0
func (c *RestClient) QueryDocumentsCrossPartition(query QueryReq) *RespQueryDocs {
	query.CrossPartitionEnabled = true
	queryPlan := c.QueryPlan(query)
	if queryPlan.Error() != nil {
		return &RespQueryDocs{RestResponse: queryPlan.RestResponse}
	}
	queryRewritten := queryPlan.QueryInfo.RewrittenQuery != ""
	pkranges := c.GetPkranges(query.DbName, query.CollName)
	if pkranges.Error() != nil {
		return &RespQueryDocs{RestResponse: pkranges.RestResponse}
	}
	if queryRewritten {
		query.Query = strings.ReplaceAll(queryPlan.QueryInfo.RewrittenQuery, "{documentdb-formattableorderbyquery-filter}", "true")
	}
	var result *RespQueryDocs
	savedContinuationToken := query.ContinuationToken
	for _, pkrange := range pkranges.Pkranges {
		query.PkRangeId = pkrange.Id
		for {
			result = c.mergeQueryResults(result, c.queryAllAndMerge(query, queryPlan), queryPlan)
			// fmt.Printf("\tDEBUG: num rows: %5d\n", result.Count)
			if result.Error() != nil || result.ContinuationToken == "" {
				break
			}
			query.ContinuationToken = result.ContinuationToken
		}
		if result.Error() != nil {
			return result
		}
		query.ContinuationToken = ""
	}
	return c.finalPrepareResult(result, queryPlan, savedContinuationToken)
}

// QueryPlan invokes Cosmos DB API to generate query plan.
//
// Available since v0.1.8
func (c *RestClient) QueryPlan(query QueryReq) *RespQueryPlan {
	method, urlEndpoint := "POST", c.endpoint+"/dbs/"+query.DbName+"/colls/"+query.CollName+"/docs"
	requestBody := make(map[string]interface{}, 0)
	requestBody[restApiParamQuery] = query.Query
	if query.Params != nil {
		requestBody[restApiParamParameters] = query.Params
	}
	req, err := c.buildJsonRequest(method, urlEndpoint, requestBody)
	if err != nil {
		return &RespQueryPlan{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+query.DbName+"/colls/"+query.CollName)
	req.Header.Set(httpHeaderContentType, "application/query+json")
	if query.MaxItemCount > 0 {
		req.Header.Set(restApiHeaderPageSize, strconv.Itoa(query.MaxItemCount))
	}
	if query.ContinuationToken != "" {
		req.Header.Set(restApiHeaderContinuation, query.ContinuationToken)
	}
	if query.ConsistencyLevel != "" {
		req.Header.Set(restApiHeaderConsistencyLevel, query.ConsistencyLevel)
	}
	if query.SessionToken != "" {
		req.Header.Set(restApiHeaderSessionToken, query.SessionToken)
	}
	req.Header.Set(restApiHeaderIsQueryPlanRequest, "True") // Caution: as of Dec-2022 "true" (lower-cased "t") does not work
	req.Header.Set(restApiHeaderSupportedQueryFeatures, "NonValueAggregate, Aggregate, Distinct, MultipleOrderBy, OffsetAndLimit, OrderBy, Top, CompositeAggregate, GroupBy, MultipleAggregates")
	req.Header.Set(restApiHeaderEnableCrossPartitionQuery, "true")
	req.Header.Set(restApiHeaderParallelizeCrossPartitionQuery, "true")
	resp := c.client.Do(req)
	result := &RespQueryPlan{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.CallErr = json.Unmarshal(result.RespBody, &result)
	}
	return result
}

// ListDocsReq specifies a list documents request.
type ListDocsReq struct {
	DbName, CollName  string
	MaxItemCount      int
	ContinuationToken string
	ConsistencyLevel  string // accepted values: "", "Strong", "Bounded", "Session" or "Eventual"
	SessionToken      string // string token used with session level consistency
	NotMatchEtag      string
	PkRangeId         string
	IsIncrementalFeed bool // (available since v0.1.9) if "true", the request is used to fetch the incremental changes to documents within the collection
}

func (c *RestClient) getChangeFeed(r ListDocsReq, req *http.Request) *RespListDocs {
	var result *RespListDocs
	for {
		resp := c.client.Do(req)
		tempResult := &RespListDocs{RestResponse: c.buildRestResponse(resp)}
		if 300 <= tempResult.StatusCode && tempResult.StatusCode < 400 {
			// not an error, the status code 3xx indicates that there is currently no item from the change feed
		} else if tempResult.CallErr == nil {
			tempResult.ContinuationToken = tempResult.RespHeader[respHeaderContinuation]
			tempResult.Etag = tempResult.RespHeader[respHeaderEtag]
			tempResult.CallErr = json.Unmarshal(tempResult.RespBody, &tempResult)
		}
		if result == nil {
			result = tempResult
		} else {
			result.ContinuationToken = tempResult.ContinuationToken
			result.Etag = tempResult.Etag
			result.SessionToken = tempResult.SessionToken
			result.RequestCharge += tempResult.RequestCharge
			result.Count += tempResult.Count
			result.Documents = append(result.Documents, tempResult.Documents...)
			if r.IsIncrementalFeed {
				sort.Slice(result.Documents, func(i, j int) bool {
					return result.Documents[i].Ts() < result.Documents[j].Ts()
				})
			}
		}
		if result.ContinuationToken == "" || r.MaxItemCount > 0 {
			break
		}
		req.Header.Set(restApiHeaderContinuation, result.ContinuationToken)
	}
	return result
}

// ListDocuments invokes Cosmos DB API to query read-feed for documents.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/list-documents.
//
// Note: if fetching incremental feed (ListDocsReq.IsIncrementalFeed = true), it is the caller responsibility to
// resubmit the request with proper value of etag (ListDocsReq.NotMatchEtag)
func (c *RestClient) ListDocuments(r ListDocsReq) *RespListDocs {
	method, urlEndpoint := "GET", c.endpoint+"/dbs/"+r.DbName+"/colls/"+r.CollName+"/docs"
	req, err := c.buildJsonRequest(method, urlEndpoint, nil)
	if err != nil {
		return &RespListDocs{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "docs", "dbs/"+r.DbName+"/colls/"+r.CollName)
	req.Header.Set(restApiHeaderEnableCrossPartitionQuery, "true")
	if r.MaxItemCount > 0 {
		req.Header.Set(restApiHeaderPageSize, strconv.Itoa(r.MaxItemCount))
	} else {
		// request chunk by chunk as it would have negative impact if we fetch a large number of documents in one go
		req.Header.Set(restApiHeaderPageSize, "100")
	}
	if r.ContinuationToken != "" {
		req.Header.Set(restApiHeaderContinuation, r.ContinuationToken)
	}
	if r.ConsistencyLevel != "" {
		req.Header.Set(restApiHeaderConsistencyLevel, r.ConsistencyLevel)
	}
	if r.SessionToken != "" {
		req.Header.Set(restApiHeaderSessionToken, r.SessionToken)
	}
	if r.NotMatchEtag != "" {
		req.Header.Set(httpHeaderIfNoneMatch, r.NotMatchEtag)
	}
	if r.PkRangeId != "" {
		req.Header.Set(restApiHeaderPartitionKeyRangeId, r.PkRangeId)
	}
	if r.IsIncrementalFeed {
		req.Header.Set(restApiHeaderIncremental, "Incremental feed")
		return c.getChangeFeed(r, req)
	}

	// fetch documents from table/collection
	var result *RespListDocs
	for {
		resp := c.client.Do(req)
		tempResult := &RespListDocs{RestResponse: c.buildRestResponse(resp)}
		if tempResult.CallErr == nil {
			tempResult.ContinuationToken = tempResult.RespHeader[respHeaderContinuation]
			tempResult.Etag = tempResult.RespHeader[respHeaderEtag]
			tempResult.CallErr = json.Unmarshal(tempResult.RespBody, &tempResult)
		}
		if result == nil {
			result = tempResult
		} else {
			result.ContinuationToken = tempResult.ContinuationToken
			result.Etag = tempResult.Etag
			result.SessionToken = tempResult.SessionToken
			result.RequestCharge += tempResult.RequestCharge
			result.Count += tempResult.Count
			result.Documents = append(result.Documents, tempResult.Documents...)
		}
		if result.ContinuationToken == "" || r.MaxItemCount > 0 {
			break
		}
		req.Header.Set(restApiHeaderContinuation, result.ContinuationToken)
	}
	return result
}

// GetOfferForResource invokes Cosmos DB API to get offer info of a resource.
//
// Available since v0.1.1
func (c *RestClient) GetOfferForResource(rid string) *RespGetOffer {
	queryResult := c.QueryOffers(`SELECT * FROM root WHERE root.offerResourceId="` + rid + `"`)
	result := &RespGetOffer{RestResponse: queryResult.RestResponse}
	if result.Error() == nil {
		if len(queryResult.Offers) == 0 {
			result.StatusCode = 404
			result.ApiErr = fmt.Errorf("offer not found; StatusCode=%d;Body=%s", result.StatusCode, result.RespBody)
		} else {
			result.OfferInfo = queryResult.Offers[0]
		}
	}
	return result
}

// QueryOffers invokes Cosmos DB API to query existing offers.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/querying-offers.
//
// Available since v0.1.1
func (c *RestClient) QueryOffers(query string) *RespQueryOffers {
	method, urlEndpoint := "POST", c.endpoint+"/offers"
	req, err := c.buildJsonRequest(method, urlEndpoint, map[string]interface{}{"query": query})
	if err != nil {
		return &RespQueryOffers{RestResponse: RestResponse{CallErr: err}}
	}
	req = c.addAuthHeader(req, method, "offers", "")
	req.Header.Set(httpHeaderContentType, "application/query+json")
	req.Header.Set(restApiHeaderIsQuery, "true")

	resp := c.client.Do(req)
	result := &RespQueryOffers{RestResponse: c.buildRestResponse(resp)}
	if result.CallErr == nil {
		result.ContinuationToken = result.RespHeader[respHeaderContinuation]
		result.CallErr = json.Unmarshal(result.RespBody, &result)
	}
	return result
}

func (c *RestClient) buildReplaceOfferContentAndHeaders(currentOffer OfferInfo, ru, maxru int) (map[string]interface{}, map[string]string) {
	headers := make(map[string]string)
	contentManualThroughput := map[string]interface{}{"offerThroughput": ru}
	contentDisableManualThroughput := map[string]interface{}{"offerThroughput": -1}
	contentAutopilotThroughput := map[string]interface{}{"offerAutopilotSettings": map[string]interface{}{"maxThroughput": maxru}}
	contentDisableAutopilotThroughput := map[string]interface{}{"offerAutopilotSettings": map[string]interface{}{"maxThroughput": -1}}
	if ru > 0 && maxru <= 0 {
		if currentOffer.IsAutopilot() {
			// change from auto-pilot to manual provisioning
			headers[restApiHeaderMigrateToManualThroughput] = "true"
			return contentDisableAutopilotThroughput, headers
		}
		return contentManualThroughput, headers
	}
	if ru <= 0 && maxru > 0 {
		if !currentOffer.IsAutopilot() {
			// change from manual to auto-pilot provisioning
			headers[restApiHeaderMigrateToAutopilotThroughput] = "true"
			return contentDisableManualThroughput, headers
		}
		return contentAutopilotThroughput, headers
	}
	// if we reach here, ru<=0 and maxru<=0
	if !currentOffer.IsAutopilot() {
		// change from manual to auto-pilot provisioning
		headers[restApiHeaderMigrateToAutopilotThroughput] = "true"
		return contentDisableManualThroughput, headers
	}
	return nil, headers
}

// ReplaceOfferForResource invokes Cosmos DB API to replace/update offer info of a resource.
//
//   - If ru > 0 and maxru <= 0: switch to manual throughput and set provisioning value to ru.
//   - If ru <= 0 and maxru > 0: switch to autopilot throughput and set max provisioning value to maxru.
//   - If ru <= 0 and maxru <= 0: switch to autopilot throughput with default provisioning value.
//
// Available since v0.1.1
func (c *RestClient) ReplaceOfferForResource(rid string, ru, maxru int) *RespReplaceOffer {
	if ru > 0 && maxru > 0 {
		return &RespReplaceOffer{
			RestResponse: RestResponse{
				ApiErr:     errors.New("either one of RU or MAXRU must be supplied, not both"),
				StatusCode: 400,
			},
		}
	}

	getResult := c.GetOfferForResource(rid)
	if getResult.Error() == nil {
		method, urlEndpoint := "PUT", c.endpoint+"/offers/"+getResult.OfferInfo.Rid
		params := map[string]interface{}{
			"offerVersion": "V2", "offerType": "Invalid",
			"resource":        getResult.OfferInfo.Resource,
			"offerResourceId": getResult.OfferInfo.OfferResourceId,
			"id":              getResult.OfferInfo.Rid,
			"_rid":            getResult.OfferInfo.Rid,
		}
		content, headers := c.buildReplaceOfferContentAndHeaders(getResult.OfferInfo, ru, maxru)
		if content == nil {
			return &RespReplaceOffer{RestResponse: getResult.RestResponse, OfferInfo: getResult.OfferInfo}
		}
		params[restApiParamContent] = content
		req, err := c.buildJsonRequest(method, urlEndpoint, params)
		if err != nil {
			return &RespReplaceOffer{RestResponse: RestResponse{CallErr: err}}
		}
		/*
		 * [btnguyen2k] 2022-02-16
		 * OfferInfo.Rid is returned from the server, but it _must_ be lower-cased when we send back to the server for
		 * issuing the 'replace-offer' request.
		 * Not sure if this is intended or a bug of Cosmos DB.
		 */
		req = c.addAuthHeader(req, method, "offers", strings.ToLower(getResult.OfferInfo.Rid))
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp := c.client.Do(req)
		result := &RespReplaceOffer{RestResponse: c.buildRestResponse(resp)}
		if result.CallErr == nil {
			if (headers[restApiHeaderMigrateToAutopilotThroughput] == "true" && maxru > 0) || (headers[restApiHeaderMigrateToManualThroughput] == "true" && ru > 0) {
				return c.ReplaceOfferForResource(rid, ru, maxru)
			}
			result.CallErr = json.Unmarshal(result.RespBody, &result.OfferInfo)
		}
		return result
	}
	return &RespReplaceOffer{RestResponse: getResult.RestResponse}
}

/*----------------------------------------------------------------------*/

// RestResponse captures the response from REST API call.
type RestResponse struct {
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
func (r RestResponse) Error() error {
	if r.CallErr != nil {
		return r.CallErr
	}
	return r.ApiErr
}

// DbInfo captures info of a Cosmos DB database.
type DbInfo struct {
	Id    string `json:"id"`     // user-generated unique name for the database
	Rid   string `json:"_rid"`   // (system generated property) _rid attribute of the database
	Ts    int64  `json:"_ts"`    // (system-generated property) _ts attribute of the database
	Self  string `json:"_self"`  // (system-generated property) _self attribute of the database
	Etag  string `json:"_etag"`  // (system-generated property) _etag attribute of the database
	Colls string `json:"_colls"` // (system-generated property) _colls attribute of the database
	Users string `json:"_users"` // (system-generated property) _users attribute of the database
}

func (db *DbInfo) toMap() map[string]interface{} {
	return map[string]interface{}{
		"id":     db.Id,
		"_rid":   db.Rid,
		"_ts":    db.Ts,
		"_self":  db.Self,
		"_etag":  db.Etag,
		"_colls": db.Colls,
		"_users": db.Users,
	}
}

// RespCreateDb captures the response from RestClient.CreateDatabase call.
type RespCreateDb struct {
	RestResponse
	DbInfo
}

// RespGetDb captures the response from RestClient.GetDatabase call.
type RespGetDb struct {
	RestResponse
	DbInfo
}

// RespDeleteDb captures the response from RestClient.DeleteDatabase call.
type RespDeleteDb struct {
	RestResponse
}

// RespListDb captures the response from RestClient.ListDatabases call.
type RespListDb struct {
	RestResponse `json:"-"`
	Count        int      `json:"_count"` // number of databases returned from the list operation
	Databases    []DbInfo `json:"Databases"`
}

// PkInfo holds partitioning configuration settings for a collection.
//
// @Available since v0.3.0
type PkInfo map[string]interface{}

func (pk PkInfo) Kind() string {
	kind, err := reddo.ToString(pk["kind"])
	if err == nil {
		return kind
	}
	return ""
}

func (pk PkInfo) Version() int {
	version, err := reddo.ToInt(pk["version"])
	if err == nil {
		return int(version)
	}
	return 0
}

func (pk PkInfo) Paths() []string {
	paths, err := reddo.ToSlice(pk["paths"], reddo.TypeString)
	if err == nil {
		return paths.([]string)
	}
	return nil
}

// CollInfo captures info of a Cosmos DB collection.
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
	PartitionKey             PkInfo                 `json:"partitionKey"`             // partitioning configuration settings for collection
	ConflictResolutionPolicy map[string]interface{} `json:"conflictResolutionPolicy"` // conflict resolution policy settings for collection
	GeospatialConfig         map[string]interface{} `json:"geospatialConfig"`         // Geo-spatial configuration settings for collection
}

func (c *CollInfo) toMap() map[string]interface{} {
	return map[string]interface{}{
		"id":                       c.Id,
		"_rid":                     c.Rid,
		"_ts":                      c.Ts,
		"_self":                    c.Self,
		"_etag":                    c.Etag,
		"_docs":                    c.Docs,
		"_sprocs":                  c.Sprocs,
		"_triggers":                c.Triggers,
		"_udfs":                    c.Udfs,
		"_conflicts":               c.Conflicts,
		"indexingPolicy":           c.IndexingPolicy,
		"partitionKey":             c.PartitionKey,
		"conflictResolutionPolicy": c.ConflictResolutionPolicy,
		"geospatialConfig":         c.GeospatialConfig,
	}
}

// RespCreateColl captures the response from RestClient.CreateCollection call.
type RespCreateColl struct {
	RestResponse
	CollInfo
}

// RespReplaceColl captures the response from RestClient.ReplaceCollection call.
type RespReplaceColl struct {
	RestResponse
	CollInfo
}

// RespGetColl captures the response from RestClient.GetCollection call.
type RespGetColl struct {
	RestResponse
	CollInfo
}

// RespDeleteColl captures the response from RestClient.DeleteCollection call.
type RespDeleteColl struct {
	RestResponse
}

// RespListColl captures the response from RestClient.ListCollections call.
type RespListColl struct {
	RestResponse `json:"-"`
	Count        int        `json:"_count"` // number of collections returned from the list operation
	Collections  []CollInfo `json:"DocumentCollections"`
}

// QueriedDocs is list of returned documents from a query such as result from RestClient.QueryDocuments call.
// A query can return a list of documents or a list of scalar values.
//
// Available since v0.1.9
type QueriedDocs []interface{}

// Merge merges this document list with another using the rule determined by supplied query plan and returns the merged list.
//
// Available since v0.2.0
func (docs QueriedDocs) Merge(queryPlan *RespQueryPlan, otherDocs QueriedDocs) QueriedDocs {
	if queryPlan != nil && queryPlan.IsGroupByQuery() {
		return docs.mergeGroupBy(queryPlan, otherDocs)
	}
	if queryPlan != nil && queryPlan.IsOrderByQuery() {
		result := docs.mergeOrderBy(queryPlan, otherDocs)
		if queryPlan.IsDistinctQuery() {
			result = result.ReduceDistinct(queryPlan)
		}
		return result
	}
	if queryPlan != nil && queryPlan.IsDistinctQuery() {
		return docs.mergeDistinct(queryPlan, otherDocs)
	}
	return append(docs, otherDocs...)
}

// mergeDistinct merges this document list with another using "distinct" rule (duplicated items removed) and returns the merged list.
//
// Available since v0.2.0
func (docs QueriedDocs) mergeDistinct(queryPlan *RespQueryPlan, otherDocs QueriedDocs) QueriedDocs {
	result := append(docs, otherDocs...)
	return result.ReduceDistinct(queryPlan)
}

// mergeGroupBy merges this document list with another using "group by" rule and returns the merged list.
//
// This function assumes the rewritten query was executed and each returned document has the following structure: `{"groupByItems": [...], payload: {...}}`.
//
// Available since v0.2.0
func (docs QueriedDocs) mergeGroupBy(queryPlan *RespQueryPlan, otherDocs QueriedDocs) QueriedDocs {
	result := make(QueriedDocs, 0)
	for _, otherDoc := range append(docs.AsDocInfoSlice(), otherDocs.AsDocInfoSlice()...) {
		merged := false
		for _, myDoc := range result.AsDocInfoSlice() {
			if reflect.DeepEqual(myDoc["groupByItems"], otherDoc["groupByItems"]) {
				myPayload := myDoc["payload"].(map[string]interface{})
				otherPayload := otherDoc["payload"].(map[string]interface{})
				for k, v := range queryPlan.QueryInfo.GroupByAliasToAggregateType {
					v = strings.ToUpper(v)
					switch v {
					case "COUNT", "SUM":
						myVal, _ := reddo.ToFloat(myPayload[k].(map[string]interface{})["item"])
						otherVal, _ := reddo.ToFloat(otherPayload[k].(map[string]interface{})["item"])
						myPayload[k].(map[string]interface{})["item"] = myVal + otherVal
						myDoc["payload"] = myPayload
					case "MAX", "MIN":
						myVal, _ := reddo.ToFloat(myPayload[k].(map[string]interface{})["item"])
						otherVal, _ := reddo.ToFloat(otherPayload[k].(map[string]interface{})["item"])
						if v == "MAX" && otherVal > myVal {
							myPayload[k].(map[string]interface{})["item"] = otherVal
							myDoc["payload"] = myPayload
						} else if v == "MIN" && otherVal < myVal {
							myPayload[k].(map[string]interface{})["item"] = otherVal
							myDoc["payload"] = myPayload
						}
					case "AVERAGE":
						mySum, _ := reddo.ToFloat(myPayload[k].(map[string]interface{})["item"].(map[string]interface{})["sum"])
						myCount, _ := reddo.ToInt(myPayload[k].(map[string]interface{})["item"].(map[string]interface{})["count"])
						otherSum, _ := reddo.ToFloat(otherPayload[k].(map[string]interface{})["item"].(map[string]interface{})["sum"])
						otherCount, _ := reddo.ToInt(otherPayload[k].(map[string]interface{})["item"].(map[string]interface{})["count"])
						myPayload[k].(map[string]interface{})["item"] = map[string]interface{}{"sum": mySum + otherSum, "count": myCount + otherCount}
						myDoc["payload"] = myPayload
					}
				}
				merged = true
				break
			}
		}
		if !merged {
			result = append(result, otherDoc)
		}
	}
	return result
}

func _convertToStrings(i, j interface{}) (string, string, bool) {
	if i == nil {
		i = ""
	}
	if j == nil {
		j = ""
	}
	istr, iok := i.(string)
	jstr, jok := j.(string)
	return istr, jstr, iok && jok
}

func _convertToFloats(i, j interface{}) (float64, float64, bool) {
	if i == nil {
		i = 0.0
	}
	if j == nil {
		j = 0.0
	}
	ifloat, iok := i.(float64)
	jfloat, jok := j.(float64)
	return ifloat, jfloat, iok && jok
}

// mergeOrderBy merges this document list with another using "order by" rule (the final list is sorted) and returns the merged list.
//
// This function assumes the rewritten query was executed and each returned document has the following structure: `{"orderByItems": [...], payload: {...}}`.
//
// Available since v0.2.0
func (docs QueriedDocs) mergeOrderBy(queryPlan *RespQueryPlan, otherDocs QueriedDocs) QueriedDocs {
	result := append(docs, otherDocs...)
	sort.Slice(result, func(i, j int) bool {
		iOrderByItems := result[i].(map[string]interface{})["orderByItems"].([]interface{})
		jOrderByItems := result[j].(map[string]interface{})["orderByItems"].([]interface{})
		for index, odir := range queryPlan.QueryInfo.OrderBy {
			odir = strings.ToUpper(odir)
			iItem := iOrderByItems[index].(map[string]interface{})["item"]
			jItem := jOrderByItems[index].(map[string]interface{})["item"]
			if iItem == jItem {
				continue
			}

			if istr, jstr, ok := _convertToStrings(iItem, jItem); ok {
				return (odir == "DESCENDING" && istr > jstr) || (odir != "DESCENDING" && istr < jstr)
			}
			if ifloat, jfloat, ok := _convertToFloats(iItem, jItem); ok {
				return (odir == "DESCENDING" && ifloat > jfloat) || (odir != "DESCENDING" && ifloat < jfloat)
			}
		}
		return false
	})
	return result
}

// AsDocInfoAt returns the i-th queried document as a DocInfo.
func (docs QueriedDocs) AsDocInfoAt(i int) DocInfo {
	switch docInfo := docs[i].(type) {
	case DocInfo:
		return docInfo
	case map[string]interface{}:
		return docInfo
	default:
		return nil
	}
}

// AsDocInfoSlice returns the queried documents as []DocInfo.
func (docs QueriedDocs) AsDocInfoSlice() []DocInfo {
	result := make([]DocInfo, len(docs))
	for i, doc := range docs {
		switch docInfo := doc.(type) {
		case DocInfo:
			result[i] = docInfo
		case map[string]interface{}:
			result[i] = docInfo
		default:
			return nil
		}
	}
	return result
}

// Flatten transforms result from execution of a rewritten query to the non-rewritten form.
//
// Available since v0.2.0
func (docs QueriedDocs) Flatten(queryPlan *RespQueryPlan) QueriedDocs {
	result := make(QueriedDocs, len(docs))
	for i, item := range docs {
		doc := item
		if queryPlan != nil && (queryPlan.IsOrderByQuery() || queryPlan.IsGroupByQuery()) {
			switch v := item.(type) {
			case map[string]interface{}:
				doc = v["payload"]
			case DocInfo:
				doc = v["payload"]
			}
			if queryPlan.IsGroupByQuery() {
				payload, ok := doc.(map[string]interface{})
				if ok {
					docDest := DocInfo{}
					for k, v := range queryPlan.QueryInfo.GroupByAliasToAggregateType {
						docDest[k] = payload[k]
						if strings.ToUpper(v) == "AVERAGE" {
							docDest[k] = 0.0
							count, _ := reddo.ToFloat(payload[k].(map[string]interface{})["item"].(map[string]interface{})["count"])
							if count != 0.0 {
								sum, _ := reddo.ToFloat(payload[k].(map[string]interface{})["item"].(map[string]interface{})["sum"])
								docDest[k] = sum / count
							}
						} else if v != "" {
							docDest[k] = payload[k].(map[string]interface{})["item"]
						}
					}
					doc = docDest
				}
			}
		}
		result[i] = doc
	}
	return result
}

// ReduceDistinct removes duplicated rows from a SELECT DISTINCT query.
//
// Available since v0.2.0
func (docs QueriedDocs) ReduceDistinct(queryPlan *RespQueryPlan) QueriedDocs {
	itemMap := make(map[string]bool)
	result := make(QueriedDocs, 0)
	queryRewritten := queryPlan.QueryInfo.RewrittenQuery != ""
	hf1, hf2 := checksum.Crc32HashFunc, checksum.Md5HashFunc // CRC32 + MD5 hashing is fast (is MD5 + SHA1 better?)
	for _, doc := range docs {
		item := doc
		if docAsMap, typOk := doc.(map[string]interface{}); typOk && queryRewritten {
			ok := false
			if item, ok = docAsMap["payload"]; !ok {
				// fallback
				item = doc
			}
		}
		key := fmt.Sprintf("%x:%x", checksum.Checksum(hf1, item), checksum.Checksum(hf2, item))
		if _, ok := itemMap[key]; !ok {
			itemMap[key] = true
			result = append(result, doc)
		}
	}
	return result
}

// ReduceGroupBy merge rows returned from a SELECT...GROUP BY "rewritten" query.
//
// Available since v0.2.0
func (docs QueriedDocs) ReduceGroupBy(queryPlan *RespQueryPlan) QueriedDocs {
	result := make(QueriedDocs, 0)
	for _, otherDoc := range docs.AsDocInfoSlice() {
		merged := false
		for _, myDoc := range result.AsDocInfoSlice() {
			if reflect.DeepEqual(myDoc["groupByItems"], otherDoc["groupByItems"]) {
				myPayload := myDoc["payload"].(map[string]interface{})
				otherPayload := otherDoc["payload"].(map[string]interface{})
				for k, v := range queryPlan.QueryInfo.GroupByAliasToAggregateType {
					v = strings.ToUpper(v)
					switch v {
					case "COUNT", "SUM":
						myVal, _ := reddo.ToFloat(myPayload[k].(map[string]interface{})["item"])
						otherVal, _ := reddo.ToFloat(otherPayload[k].(map[string]interface{})["item"])
						myPayload[k].(map[string]interface{})["item"] = myVal + otherVal
						myDoc["payload"] = myPayload
					case "MAX", "MIN":
						myVal, _ := reddo.ToFloat(myPayload[k].(map[string]interface{})["item"])
						otherVal, _ := reddo.ToFloat(otherPayload[k].(map[string]interface{})["item"])
						if v == "MAX" && otherVal > myVal {
							myPayload[k].(map[string]interface{})["item"] = otherVal
							myDoc["payload"] = myPayload
						} else if v == "MIN" && otherVal < myVal {
							myPayload[k].(map[string]interface{})["item"] = otherVal
							myDoc["payload"] = myPayload
						}
					case "AVERAGE":
						mySum, _ := reddo.ToFloat(myPayload[k].(map[string]interface{})["item"].(map[string]interface{})["sum"])
						myCount, _ := reddo.ToInt(myPayload[k].(map[string]interface{})["item"].(map[string]interface{})["count"])
						otherSum, _ := reddo.ToFloat(otherPayload[k].(map[string]interface{})["item"].(map[string]interface{})["sum"])
						otherCount, _ := reddo.ToInt(otherPayload[k].(map[string]interface{})["item"].(map[string]interface{})["count"])
						myPayload[k].(map[string]interface{})["item"] = map[string]interface{}{"sum": mySum + otherSum, "count": myCount + otherCount}
						myDoc["payload"] = myPayload
					}
				}
				merged = true
				break
			}
		}
		if !merged {
			result = append(result, otherDoc)
		}
	}
	return result
}

// DocInfo is a Cosmos DB document.
type DocInfo map[string]interface{}

// AsMap return the document as [string]interface{}
//
// Available since v0.1.9
func (d DocInfo) AsMap() map[string]interface{} {
	return d
}

// RemoveSystemAttrs returns a clone of the document with all system attributes removed.
func (d DocInfo) RemoveSystemAttrs() DocInfo {
	clone := DocInfo{}
	for k, v := range d {
		if !strings.HasPrefix(k, "_") {
			clone[k] = v
		}
	}
	return clone
}

// GetAttrAsType returns a document attribute converting to a specific type.
//
// Note: if typ is nil, the attribute value is returned as-is (i.e. without converting).
func (d DocInfo) GetAttrAsType(attrName string, typ reflect.Type) (interface{}, error) {
	v, ok := d[attrName]
	if ok && v != nil {
		return reddo.Convert(v, typ)
	}
	return nil, nil
}

// Id returns the value of document's "id" attribute.
func (d DocInfo) Id() string {
	v := d.GetAttrAsTypeUnsafe("id", reddo.TypeString)
	if v != nil {
		return v.(string)
	}
	return ""
}

// Rid returns the value of document's "_rid" attribute.
func (d DocInfo) Rid() string {
	v := d.GetAttrAsTypeUnsafe("_rid", reddo.TypeString)
	if v != nil {
		return v.(string)
	}
	return ""
}

// Attachments returns the value of document's "_attachments" attribute.
func (d DocInfo) Attachments() string {
	v := d.GetAttrAsTypeUnsafe("_attachments", reddo.TypeString)
	if v != nil {
		return v.(string)
	}
	return ""
}

// Etag returns the value of document's "_etag" attribute.
func (d DocInfo) Etag() string {
	v := d.GetAttrAsTypeUnsafe("_etag", reddo.TypeString)
	if v != nil {
		return v.(string)
	}
	return ""
}

// Self returns the value of document's "_self" attribute.
func (d DocInfo) Self() string {
	v := d.GetAttrAsTypeUnsafe("_self", reddo.TypeString)
	if v != nil {
		return v.(string)
	}
	return ""
}

// Ts returns the value of document's "_ts" attribute.
func (d DocInfo) Ts() int64 {
	v := d.GetAttrAsTypeUnsafe("_ts", reddo.TypeInt)
	if v != nil {
		return v.(int64)
	}
	return 0
}

// TsAsTime returns the value of document's "_ts" attribute as a time.Time.
func (d DocInfo) TsAsTime() time.Time {
	return time.Unix(d.Ts(), 0)
}

// GetAttrAsTypeUnsafe is similar to GetAttrAsType except that it does not check for error.
func (d DocInfo) GetAttrAsTypeUnsafe(attrName string, typ reflect.Type) interface{} {
	v, _ := d.GetAttrAsType(attrName, typ)
	return v
}

// RespCreateDoc captures the response from RestClient.CreateDocument call.
type RespCreateDoc struct {
	RestResponse
	DocInfo
}

// RespReplaceDoc captures the response from RestClient.ReplaceDocument call.
type RespReplaceDoc struct {
	RestResponse
	DocInfo
}

// RespGetDoc captures the response from RestClient.GetDocument call.
type RespGetDoc struct {
	RestResponse
	DocInfo
}

// RespDeleteDoc captures the response from RestClient.DeleteDocument call.
type RespDeleteDoc struct {
	RestResponse
}

// RespQueryDocs captures the response from RestClient.QueryDocuments call.
type RespQueryDocs struct {
	RestResponse       `json:"-"`
	Count              int            `json:"_count"` // number of documents returned from the operation
	Documents          QueriedDocs    `json:"Documents"`
	ContinuationToken  string         `json:"-"`
	QueryPlan          *RespQueryPlan `json:"-"` // (available since v0.2.0) the query plan used to execute the query
	RewrittenDocuments QueriedDocs    `json:"-"` // (available since v0.2.0) the original returned documents from the execution of RespQueryPlan.QueryInfo.RewrittenQuery
}

// Available since v0.2.0
func (r *RespQueryDocs) populateRewrittenDocuments(queryPlan *RespQueryPlan) *RespQueryDocs {
	r.QueryPlan = queryPlan
	r.RewrittenDocuments = nil
	if queryPlan != nil && queryPlan.QueryInfo.RewrittenQuery != "" {
		r.RewrittenDocuments = make(QueriedDocs, len(r.Documents))
		copy(r.RewrittenDocuments, r.Documents)
	}
	return r
}

// note: this function does NOT add up request-charge
// note 2: we are executing the rewritten query (if any)
func (r *RespQueryDocs) merge(queryPlan *RespQueryPlan, other *RespQueryDocs) *RespQueryDocs {
	r.Documents = r.Documents.Merge(queryPlan, other.Documents)
	r.Count = len(r.Documents)
	return r
}

type typDCountInfo struct {
	DCountAlias string `json:"dCountAlias"`
}

// RespQueryPlan captures the response from QueryPlan call.
//
// Available since v0.1.8
type RespQueryPlan struct {
	RestResponse              `json:"-"`
	QueryExecutionInfoVersion int `json:"partitionedQueryExecutionInfoVersion"`
	QueryInfo                 struct {
		DistinctType                string            `json:"distinctType"` // possible values: None, Ordered, Unordered
		Top                         int               `json:"top"`
		Offset                      int               `json:"offset"`
		Limit                       int               `json:"limit"`
		OrderBy                     []string          `json:"orderBy"` // possible values: Ascending, Descending
		OrderByExpressions          []string          `json:"orderByExpressions"`
		GroupByExpressions          []string          `json:"groupByExpressions"`
		GroupByAliases              []string          `json:"groupByAliases"`
		Aggregates                  []string          `json:"aggregates"` // possible values: Average, Count, Max, Min, Sum
		GroupByAliasToAggregateType map[string]string `json:"groupByAliasToAggregateType"`
		RewrittenQuery              string            `json:"rewrittenQuery"`
		HasSelectValue              bool              `json:"hasSelectValue"`
		DCountInfo                  typDCountInfo     `json:"dCountInfo"`
	} `json:"queryInfo"`
}

// IsDistinctQuery tests if duplicates are eliminated in the query's projection.
//
// Available v0.1.9
func (qp *RespQueryPlan) IsDistinctQuery() bool {
	return strings.ToUpper(qp.QueryInfo.DistinctType) != "NONE"
}

// IsGroupByQuery tests if "group-by" aggregation is in the query's projection.
//
// Available v0.1.9
func (qp *RespQueryPlan) IsGroupByQuery() bool {
	return len(qp.QueryInfo.GroupByAliasToAggregateType) > 0
}

// IsOrderByQuery tests if "order-by" clause is in the query's projection.
//
// Available v0.1.9
func (qp *RespQueryPlan) IsOrderByQuery() bool {
	return len(qp.QueryInfo.OrderByExpressions) > 0
}

// RespListDocs captures the response from RestClient.ListDocuments call.
type RespListDocs struct {
	RestResponse      `json:"-"`
	Count             int       `json:"_count"` // number of documents returned from the operation
	Documents         []DocInfo `json:"Documents"`
	ContinuationToken string    `json:"-"`
	Etag              string    `json:"-"` // logical sequence number (LSN) of last document returned in the response
}

// OfferInfo captures info of a Cosmos DB offer.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/offers.
type OfferInfo struct {
	OfferVersion    string                 `json:"offerVersion"`    // V2 is the current version for request unit-based throughput.
	OfferType       string                 `json:"offerType"`       // This value indicates the performance level for V1 offer version, allowed values for V1 offer are S1, S2, or S3. This property is set to Invalid for V2 offer version.
	Content         map[string]interface{} `json:"content"`         // Contains information about the offer – for V2 offers, this contains the throughput of the collection.
	Resource        string                 `json:"resource"`        // When creating a new collection, this property is set to the self-link of the collection.
	OfferResourceId string                 `json:"offerResourceId"` // During creation of a collection, this property is automatically associated to the resource ID, that is, _rid of the collection.
	Id              string                 `json:"id"`              // It is a system-generated property. The ID for the offer resource is automatically generated when it is created. It has the same value as the _rid for the offer.
	Rid             string                 `json:"_rid"`            // It is a system-generated property. The resource ID (_rid) is a unique identifier that is also hierarchical per the resource stack on the resource model. It is used internally for placement and navigation of the offer.
	Ts              int64                  `json:"_ts"`             // It is a system-generated property. It specifies the last updated timestamp of the resource. The value is a timestamp.
	Self            string                 `json:"_self"`           // It is a system-generated property. It is the unique addressable URI for the resource.
	Etag            string                 `json:"_etag"`           // It is a system-generated property that specifies the resource etag required for optimistic concurrency control.
	//_lock           sync.Mutex
	_s *semita.Semita
}

// OfferThroughput returns value of field 'offerThroughput'
func (o OfferInfo) OfferThroughput() int {
	//o._lock.Lock()
	//defer o._lock.Unlock()
	if o._s == nil {
		o._s = semita.NewSemita(o.Content)
	}
	v, err := o._s.GetValueOfType("offerThroughput", reddo.TypeInt)
	if err == nil {
		return int(v.(int64))
	}
	return 0
}

// MaxThroughputEverProvisioned returns value of field 'maxThroughputEverProvisioned'
func (o OfferInfo) MaxThroughputEverProvisioned() int {
	//o._lock.Lock()
	//defer o._lock.Unlock()
	if o._s == nil {
		o._s = semita.NewSemita(o.Content)
	}
	v, err := o._s.GetValueOfType("offerMinimumThroughputParameters.maxThroughputEverProvisioned", reddo.TypeInt)
	if err == nil {
		return int(v.(int64))
	}
	return 0
}

// IsAutopilot returns true if autopilot is enabled, false otherwise.
func (o OfferInfo) IsAutopilot() bool {
	//o._lock.Lock()
	//defer o._lock.Unlock()
	if o._s == nil {
		o._s = semita.NewSemita(o.Content)
	}
	v, err := o._s.GetValue("offerAutopilotSettings")
	return err == nil && v != nil
}

// RespGetOffer captures the response from RestClient.GetOffer call.
type RespGetOffer struct {
	RestResponse
	OfferInfo
}

// RespQueryOffers captures the response from RestClient.QueryOffers call.
type RespQueryOffers struct {
	RestResponse      `json:"-"`
	Count             int         `json:"_count"` // number of records returned from the operation
	Offers            []OfferInfo `json:"Offers"`
	ContinuationToken string      `json:"-"`
}

// RespReplaceOffer captures the response from RestClient.ReplaceOffer call.
type RespReplaceOffer struct {
	RestResponse
	OfferInfo
}

// PkrangeInfo captures info of a collection's partition key range.
//
// See: https://docs.microsoft.com/en-us/rest/api/cosmos-db/get-partition-key-ranges.
//
// Available since v0.1.3.
type PkrangeInfo struct {
	Id           string `json:"id"`           // the stable and unique ID for the partition key range within each collection
	MaxExclusive string `json:"maxExclusive"` // (internal use) the maximum partition key hash value for the partition key range
	MinInclusive string `json:"minInclusive"` // (minimum use) the maximum partition key hash value for the partition key range
	Rid          string `json:"_rid"`         // (system generated property) _rid attribute of the pkrange
	Ts           int64  `json:"_ts"`          // (system-generated property) _ts attribute of the pkrange
	Self         string `json:"_self"`        // (system-generated property) _self attribute of the pkrange
	Etag         string `json:"_etag"`        // (system-generated property) _etag attribute of the pkrange
}

// RespGetPkranges captures the response from GetPkranges call.
//
// Available since v0.1.3.
type RespGetPkranges struct {
	RestResponse `json:"-"`
	Pkranges     []PkrangeInfo `json:"PartitionKeyRanges"`
	Count        int           `json:"_count"` // number of records returned from the operation
}
