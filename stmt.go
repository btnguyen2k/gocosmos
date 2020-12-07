package go_cosmos

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/btnguyen2k/consu/gjrc"
	"github.com/btnguyen2k/consu/reddo"
)

var (
	regExpA = regexp.MustCompile(`@\d+`)
	regExpC = regexp.MustCompile(`:\d+`)
	regExpD = regexp.MustCompile(`\$\d+`)

	regExpCreateDb = regexp.MustCompile(`(?i)^CREATE\s+DATABASE(\s+IF\s+NOT\s+EXISTS)?\s+(\w+)(\s+WITH\s+RU\s*=\s*(\d+))?$`)
	regExpDropDb   = regexp.MustCompile(`(?i)^DROP\s+DATABASE(\s+IF\s+EXISTS)?\s+(\w+)$`)
)

func parseQuery(c *Conn, query string) (driver.Stmt, error) {
	query = strings.TrimSpace(query)
	if regExpCreateDb.MatchString(query) {
		groups := regExpCreateDb.FindAllStringSubmatch(query, -1)
		dbName := groups[0][2]
		ifNotExists := groups[0][1] != ""
		ru, _ := strconv.ParseInt(groups[0][4], 10, 64)
		return &StmtCreateDatabase{
			Stmt: &Stmt{
				query:    query,
				conn:     c,
				numInput: 0,
			},
			dbName:      dbName,
			ifNotExists: ifNotExists,
			ru:          int(ru),
		}, nil
	}
	if regExpDropDb.MatchString(query) {
		groups := regExpDropDb.FindAllStringSubmatch(query, -1)
		dbName := groups[0][2]
		ifExists := groups[0][1] != ""
		return &StmtDropDatabase{
			Stmt: &Stmt{
				query:    query,
				conn:     c,
				numInput: 0,
			},
			dbName:   dbName,
			ifExists: ifExists,
		}, nil
	}

	numInput := 0
	for _, regExp := range []*regexp.Regexp{regExpA, regExpC, regExpD} {
		numInput += len(regExp.FindAllString(query, -1))
	}
	stmt := &Stmt{
		query:    query,
		conn:     c,
		numInput: numInput,
	}
	return stmt, nil
}

// Stmt is Azure CosmosDB prepared statement handle.
type Stmt struct {
	query    string // the SQL query
	conn     *Conn  // the connection that this prepared statement is bound to
	numInput int    // number of placeholder parameters
}

// NumInput implements driver.Stmt.Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt.NumInput.
func (s *Stmt) NumInput() int {
	return s.numInput
}

// Exec implements driver.Stmt.Exec.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("[Exec] implement me")
}

// Query implements driver.Stmt.Query.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("[Query] implement me")
}

func (s *Stmt) buildError(resp *gjrc.GjrcResponse) error {
	if resp.Error() != nil {
		return resp.Error()
	}
	if resp.StatusCode() >= 400 {
		body, _ := resp.Body()
		return fmt.Errorf("error executing Azure CosmosDB command; StatusCode=%d;Body=%s", resp.StatusCode(), body)
	}
	return nil
}

/*----------------------------------------------------------------------*/

// StmtCreateDatabase implements "CREATE DATABASE" query.
//
// Syntax: CREATE DATABASE [IF NOT EXISTS] <db-name> [WITH RU=ru]
//
// - ru: an integer specifying CosmosDB's database throughput expressed in RU/s.
// - if "IF NOT EXISTS" is specified, Exec will silently swallow the error "409 Conflict".
type StmtCreateDatabase struct {
	*Stmt
	dbName      string
	ifNotExists bool
	ru          int
}

// Query implements driver.Stmt.Query.
func (s *StmtCreateDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtCreateDatabase) Exec(_ []driver.Value) (driver.Result, error) {
	method := "POST"
	url := s.conn.endpoint + "/dbs"
	req := s.conn.buildJsonRequest(method, url, map[string]interface{}{"id": s.dbName})
	req = s.conn.addAuthHeader(req, method, "dbs", "")
	if s.ru > 0 {
		req.Header.Set("x-ms-offer-throughput", strconv.Itoa(s.ru))
	}

	resp := s.conn.client.Do(req)
	err := s.buildError(resp)
	result := &ResultCreateDatabase{Successful: err == nil, StatusCode: resp.StatusCode()}
	if err == nil {
		rid, _ := resp.GetValueAsType("_rid", reddo.TypeString)
		result.InsertId = rid.(string)
	}
	if err != nil && resp.StatusCode() == 409 && s.ifNotExists {
		err = nil
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

// StmtDropDatabase implements "DROP DATABASE" query.
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
func (s *StmtDropDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtDropDatabase) Exec(_ []driver.Value) (driver.Result, error) {
	method := "DELETE"
	url := s.conn.endpoint + "/dbs/" + s.dbName
	req := s.conn.buildJsonRequest(method, url, nil)
	req = s.conn.addAuthHeader(req, method, "dbs", "dbs/"+s.dbName)

	resp := s.conn.client.Do(req)
	err := s.buildError(resp)
	if err != nil && resp.StatusCode() == 404 && s.ifExists {
		err = nil
	}
	return nil, err
}
