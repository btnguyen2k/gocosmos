package go_cosmos

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/btnguyen2k/consu/reddo"
)

// StmtCreateDatabase implements "CREATE DATABASE" operation.
//
// Syntax: CREATE DATABASE [IF NOT EXISTS] <db-name> [WITH RU|MAXRU=ru]
//
// - ru: an integer specifying CosmosDB's database throughput expressed in RU/s.
// - if "IF NOT EXISTS" is specified, Exec will silently swallow the error "409 Conflict".
type StmtCreateDatabase struct {
	*Stmt
	dbName      string
	ifNotExists bool
	ru, maxru   int
}

func (s *StmtCreateDatabase) parseWithOpts(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}
	if _, ok := s.withOpts["RU"]; ok {
		if ru, err := strconv.ParseInt(s.withOpts["RU"], 10, 64); err != nil || ru < 0 {
			return fmt.Errorf("invalid RU value: %s", s.withOpts["RU"])
		} else {
			s.ru = int(ru)
		}
	}
	if _, ok := s.withOpts["MAXRU"]; ok {
		if maxru, err := strconv.ParseInt(s.withOpts["MAXRU"], 10, 64); err != nil || maxru < 0 {
			return fmt.Errorf("invalid MAXRU value: %s", s.withOpts["MAXRU"])
		} else {
			s.maxru = int(maxru)
		}
	}
	return nil
}

func (s *StmtCreateDatabase) validateWithOpts() error {
	if s.ru > 0 && s.maxru > 0 {
		return errors.New("only one of RU or MAXRU must be specified")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
// Upon successful call, this function return (*ResultCreateDatabase, nil)
func (s *StmtCreateDatabase) Exec(_ []driver.Value) (driver.Result, error) {
	method := "POST"
	url := s.conn.endpoint + "/dbs"
	req := s.conn.buildJsonRequest(method, url, map[string]interface{}{"id": s.dbName})
	req = s.conn.addAuthHeader(req, method, "dbs", "")
	if s.ru > 0 {
		req.Header.Set("x-ms-offer-throughput", strconv.Itoa(s.ru))
	}
	if s.maxru > 0 {
		req.Header.Set("x-ms-cosmos-offer-autopilot-settings", fmt.Sprintf(`{"maxThroughput":%d}`, s.maxru))
	}

	resp := s.conn.client.Do(req)
	err, statusCode := s.buildError(resp)
	result := &ResultCreateDatabase{Successful: err == nil, StatusCode: statusCode}
	if err == nil {
		rid, _ := resp.GetValueAsType("_rid", reddo.TypeString)
		result.InsertId = rid.(string)
	}
	switch statusCode {
	case 403:
		err = ErrForbidden
	case 409:
		if s.ifNotExists {
			err = nil
		} else {
			err = ErrConflict
		}
	}
	return result, err
}

// ResultCreateDatabase captures the result from CREATE DATABASE operation.
type ResultCreateDatabase struct {
	// Successful flags if the operation was successful or not.
	Successful bool
	// StatusCode is the HTTP status code returned from CosmosDB.
	StatusCode int
	// InsertId holds the "_rid" if the operation was successful.
	InsertId string
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultCreateDatabase) LastInsertId() (int64, error) {
	return 0, errors.New("this operation is not supported, please read _rid value from field InsertId")
}

// LastInsertId implements driver.Result.RowsAffected.
func (r *ResultCreateDatabase) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDropDatabase implements "DROP DATABASE" operation.
//
// Syntax: DROP DATABASE [IF EXISTS] <db-name>
//
// - if "IF EXISTS" is specified, Exec will silently swallow the error "404 Not Found".
type StmtDropDatabase struct {
	*Stmt
	dbName   string
	ifExists bool
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
// This function always return a nil driver.Result.
func (s *StmtDropDatabase) Exec(_ []driver.Value) (driver.Result, error) {
	method := "DELETE"
	url := s.conn.endpoint + "/dbs/" + s.dbName
	req := s.conn.buildJsonRequest(method, url, nil)
	req = s.conn.addAuthHeader(req, method, "dbs", "dbs/"+s.dbName)

	resp := s.conn.client.Do(req)
	err, statusCode := s.buildError(resp)
	switch statusCode {
	case 403:
		err = ErrForbidden
	case 404:
		if s.ifExists {
			err = nil
		} else {
			err = ErrNotFound
		}
	}
	return nil, err
}

/*----------------------------------------------------------------------*/

// StmtListDatabases implements "LIST DATABASES" operation.
//
// Syntax: LIST DATABASES|DATABASE
type StmtListDatabases struct {
	*Stmt
}

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtListDatabases) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use query")
}

// Query implements driver.Stmt.Query.
func (s *StmtListDatabases) Query(_ []driver.Value) (driver.Rows, error) {
	method := "GET"
	url := s.conn.endpoint + "/dbs"
	req := s.conn.buildJsonRequest(method, url, nil)
	req = s.conn.addAuthHeader(req, method, "dbs", "")

	resp := s.conn.client.Do(req)
	err, statusCode := s.buildError(resp)
	var rows driver.Rows
	if err == nil {
		body, _ := resp.Body()
		var listDbResult listDbResult
		err = json.Unmarshal(body, &listDbResult)
		sort.Slice(listDbResult.Databases, func(i, j int) bool {
			// sort databases by id
			return listDbResult.Databases[i].Id < listDbResult.Databases[j].Id
		})
		rows = &RowsListDatabases{result: listDbResult, cursorCount: 0}
	}
	switch statusCode {
	case 403:
		err = ErrForbidden
	case 404:
		err = ErrNotFound
	}
	return rows, err
}

type dbInfo struct {
	Id    string `json:"id"`
	Rid   string `json:"_rid"`
	Ts    int    `json:"_ts"`
	Self  string `json:"_self"`
	Etag  string `json:"_etag"`
	Colls string `json:"_colls"`
	Users string `json:"_users"`
}

type listDbResult struct {
	Rid       string   `json:"_rid"`
	Databases []dbInfo `json:"Databases"`
	Count     int      `json:"_count"`
}

// RowsListDatabases captures the result from LIST DATABASES operation.
type RowsListDatabases struct {
	result      listDbResult
	cursorCount int
}

// Columns implements driver.Rows.Columns.
func (r *RowsListDatabases) Columns() []string {
	return []string{"id", "_rid", "_ts", "_self", "_etag", "_colls", "_users"}
}

// Close implements driver.Rows.Close.
func (r *RowsListDatabases) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *RowsListDatabases) Next(dest []driver.Value) error {
	if r.cursorCount >= len(r.result.Databases) {
		return io.EOF
	}
	rowData := r.result.Databases[r.cursorCount]
	r.cursorCount++
	dest[0] = rowData.Id
	dest[1] = rowData.Rid
	dest[2] = rowData.Ts
	dest[3] = rowData.Self
	dest[4] = rowData.Etag
	dest[5] = rowData.Colls
	dest[6] = rowData.Users
	return nil
}
