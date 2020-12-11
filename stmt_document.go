package go_cosmos

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/btnguyen2k/consu/reddo"
)

var (
	reValNull        = regexp.MustCompile(`(?i)(null)\s*,?`)
	reValNumber      = regexp.MustCompile(`([\d\.xe+-]+)\s*,?`)
	reValBoolean     = regexp.MustCompile(`(?i)(true|false)\s*,?`)
	reValString      = regexp.MustCompile(`(?i)("(\\"|[^"])*?")\s*,?`)
	reValPlaceholder = regexp.MustCompile(`(?i)[$@:](\d+)\s*,?`)
)

type placeholder struct {
	index int
}

// StmtInsert implements "INSERT" operation.
//
// Syntax: INSERT INTO <db-name>.<collection-name> (<field-list>) VALUES (<value-list>)
//
// - values are comma separated.
// - a value is either:
//   - a placeholder (e.g. :1, @2 or $3)
//   - a null
//   - a number
//   - a boolean (true/false)
//   - a string (inside double quotes) that must be a valid JSON, e.g.
//     - a string value in JSON (include the double quotes): "\"a string\""
//     - a number value in JSON (include the double quotes): "123"
//     - a boolean value in JSON (include the double quotes): "true"
//     - a null value in JSON (include the double quotes): "null"
//     - a map value in JSON (include the double quotes): "{\"key\":\"value\"}"
//     - a list value in JSON (include the double quotes): "[1,true,nil,\"string\"]"
type StmtInsert struct {
	*Stmt
	dbName    string
	collName  string
	fieldsStr string
	valuesStr string
	fields    []string
	values    []interface{}
}

func (s *StmtInsert) parse() error {
	s.fields = regexp.MustCompile(`[,\s]+`).Split(s.fieldsStr, -1)
	s.values = make([]interface{}, 0)
	s.numInput = 1
	for temp := strings.TrimSpace(s.valuesStr); temp != ""; temp = strings.TrimSpace(temp) {
		if loc := reValPlaceholder.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token := strings.Trim(temp[loc[0]+1:loc[1]], " ,")
			s.numInput++
			index, _ := strconv.Atoi(token)
			s.values = append(s.values, placeholder{index})
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValNull.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			s.values = append(s.values, nil)
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValNumber.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token := strings.Trim(temp[loc[0]:loc[1]], " ,")
			var data interface{}
			if err := json.Unmarshal([]byte(token), &data); err != nil {
				return errors.New("(nul) cannot parse query, invalid token at: " + token)
			}
			s.values = append(s.values, data)
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValBoolean.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token := strings.Trim(temp[loc[0]:loc[1]], " ,")
			var data bool
			if err := json.Unmarshal([]byte(token), &data); err != nil {
				return errors.New("(bool) cannot parse query, invalid token at: " + token)
			}
			s.values = append(s.values, data)
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValString.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token, err := strconv.Unquote(strings.Trim(temp[loc[0]:loc[1]], " ,"))
			if err != nil {
				return errors.New("(unquote) cannot parse query, invalid token at: " + token)
			}
			var data interface{}
			if err := json.Unmarshal([]byte(token), &data); err != nil {
				return errors.New("(unmarshal) cannot parse query, invalid token at: " + token)
			}
			s.values = append(s.values, data)
			temp = temp[loc[1]:]
			continue
		}
		return errors.New("cannot parse query, invalid token at: " + temp)
	}

	return nil
}

func (s *StmtInsert) validate() error {
	if len(s.fields) != len(s.values) {
		return fmt.Errorf("number of field (%d) does not match number of input value (%d)", len(s.fields), len(s.values))
	}
	return nil
}

// Exec implements driver.Stmt.Exec.
// Upon successful call, this function returns (*ResultInsert, nil).
func (s *StmtInsert) Exec(args []driver.Value) (driver.Result, error) {
	method := "POST"
	url := s.conn.endpoint + "/dbs/" + s.dbName + "/colls/" + s.collName + "/docs"
	params := make(map[string]interface{})
	for i := 0; i < len(s.fields); i++ {
		switch s.values[i].(type) {
		case placeholder:
			ph := s.values[i].(placeholder)
			if ph.index <= 0 || ph.index >= len(args) {
				return nil, fmt.Errorf("invalid value index %d", ph.index)
			}
			params[s.fields[i]] = args[ph.index-1]
		case *placeholder:
			ph := s.values[i].(*placeholder)
			if ph.index <= 0 || ph.index >= len(args) {
				return nil, fmt.Errorf("invalid value index %d", ph.index)
			}
			params[s.fields[i]] = args[ph.index-1]
		default:
			params[s.fields[i]] = s.values[i]
		}
	}
	req := s.conn.buildJsonRequest(method, url, params)
	req = s.conn.addAuthHeader(req, method, "docs", "dbs/"+s.dbName+"/colls/"+s.collName)
	pkHeader := []interface{}{args[s.numInput-1]} // expect the last argument is partition key value
	jsPkHeader, _ := json.Marshal(pkHeader)
	req.Header.Set("x-ms-documentdb-partitionkey", string(jsPkHeader))

	resp := s.conn.client.Do(req)
	err, statusCode := s.buildError(resp)
	result := &ResultInsert{Successful: err == nil, StatusCode: statusCode}
	if err == nil {
		result.RUCharge, _ = strconv.ParseFloat(resp.HttpResponse().Header.Get("x-ms-request-charge"), 64)
		result.SessionToken = resp.HttpResponse().Header.Get("x-ms-session-token")
		rid, _ := resp.GetValueAsType("_rid", reddo.TypeString)
		result.InsertId = rid.(string)
	}
	switch statusCode {
	case 403:
		err = ErrForbidden
	case 404:
		err = ErrNotFound
	case 409:
		err = ErrConflict
	}
	return result, err
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtInsert) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// ResultInsert captures the result from INSERT operation.
type ResultInsert struct {
	// Successful flags if the operation was successful or not.
	Successful bool
	// StatusCode is the HTTP status code returned from CosmosDB.
	StatusCode int
	// InsertId holds the "_rid" if the operation was successful.
	InsertId string
	// RUCharge holds the number of request units consumed by the operation.
	RUCharge float64
	// SessionToken is the string token used with session level consistency.
	// Clients must save this value and set it for subsequent read requests for session consistency.
	SessionToken string
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultInsert) LastInsertId() (int64, error) {
	return 0, errors.New("this operation is not supported, please read _rid value from field InsertId")
}

// LastInsertId implements driver.Result.RowsAffected.
func (r *ResultInsert) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDelete implements "DELETE" operation.
//
// Syntax: DELETE <db-name>.<collection-name> WHERE id=<id-value>
//
// - currently DELETE only removes one document specified by id.
// - <id-value> is treated as a string. Either `WHERE id=abc` or `WHERE id="abc"` is accepted.
type StmtDelete struct {
	*Stmt
	dbName   string
	collName string
	id       string
}
