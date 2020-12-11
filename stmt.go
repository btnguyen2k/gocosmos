package go_cosmos

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"

	"github.com/btnguyen2k/consu/gjrc"
)

const (
	field       = `([\w\-]+)`
	ifNotExists = `(\s+IF\s+NOT\s+EXISTS)?`
	ifExists    = `(\s+IF\s+EXISTS)?`
	with        = `((\s+WITH\s+([\w-]+)\s*=\s*([\w/\.,;:'"-]+))*)`
)

var (
	reCreateDb = regexp.MustCompile(`(?i)^CREATE\s+DATABASE` + ifNotExists + `\s+` + field + with + `$`)
	reDropDb   = regexp.MustCompile(`(?i)^DROP\s+DATABASE` + ifExists + `\s+` + field + `$`)
	reListDbs  = regexp.MustCompile(`(?i)^LIST\s+DATABASES?$`)

	reCreateColl = regexp.MustCompile(`(?i)^CREATE\s+(COLLECTION|TABLE)` + ifNotExists + `\s+` + field + `\.` + field + with + `$`)
	reDropColl   = regexp.MustCompile(`(?i)^DROP\s+(COLLECTION|TABLE)` + ifExists + `\s+` + field + `\.` + field + `$`)
	reListColls  = regexp.MustCompile(`(?i)^LIST\s+(COLLECTIONS?|TABLES?)\s+FROM\s+` + field + `$`)

	reInsert = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+` + field + `\.` + field + `\s*\(([^)]*?)\)\s*VALUES\s*\(([^)]*?)\)$`)
	reDelete = regexp.MustCompile(`(?i)^DELETE\s+FROM\s+` + field + `\.` + field + `\s*WHERE\s+id\s*=\s*(.*)?$`)
)

func parseQuery(c *Conn, query string) (driver.Stmt, error) {
	query = strings.TrimSpace(query)
	if re := reCreateDb; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateDatabase{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			dbName:      strings.TrimSpace(groups[0][2]),
			ifNotExists: strings.TrimSpace(groups[0][1]) != "",
		}
		if err := stmt.parseWithOpts(groups[0][3]); err != nil {
			return nil, err
		}
		return stmt, stmt.validateWithOpts()
	}
	if re := reDropDb; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropDatabase{
			Stmt:     &Stmt{query: query, conn: c, numInput: 0},
			dbName:   strings.TrimSpace(groups[0][2]),
			ifExists: strings.TrimSpace(groups[0][1]) != "",
		}
		return stmt, stmt.validateWithOpts()
	}
	if re := reListDbs; re.MatchString(query) {
		stmt := &StmtListDatabases{
			Stmt: &Stmt{query: query, conn: c, numInput: 0},
		}
		return stmt, stmt.validateWithOpts()
	}

	if re := reCreateColl; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateCollection{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			ifNotExists: strings.TrimSpace(groups[0][2]) != "",
			dbName:      strings.TrimSpace(groups[0][3]),
			collName:    strings.TrimSpace(groups[0][4]),
		}
		if err := stmt.parseWithOpts(groups[0][5]); err != nil {
			return nil, err
		}
		return stmt, stmt.validateWithOpts()
	}
	if re := reDropColl; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropCollection{
			Stmt:     &Stmt{query: query, conn: c, numInput: 0},
			dbName:   strings.TrimSpace(groups[0][3]),
			collName: strings.TrimSpace(groups[0][4]),
			ifExists: strings.TrimSpace(groups[0][2]) != "",
		}
		return stmt, stmt.validateWithOpts()
	}
	if re := reListColls; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtListCollections{
			Stmt:   &Stmt{query: query, conn: c, numInput: 0},
			dbName: strings.TrimSpace(groups[0][2]),
		}
		return stmt, stmt.validateWithOpts()
	}

	if re := reInsert; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtInsert{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			dbName:    strings.TrimSpace(groups[0][1]),
			collName:  strings.TrimSpace(groups[0][2]),
			fieldsStr: strings.TrimSpace(groups[0][3]),
			valuesStr: strings.TrimSpace(groups[0][4]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDelete; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDelete{
			Stmt:     &Stmt{query: query, conn: c, numInput: 0},
			dbName:   strings.TrimSpace(groups[0][1]),
			collName: strings.TrimSpace(groups[0][2]),
			id:       strings.TrimSpace(groups[0][3]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}

	return nil, fmt.Errorf("invalid query: %s", query)
}

// Stmt is Azure CosmosDB prepared statement handle.
type Stmt struct {
	query    string // the SQL query
	conn     *Conn  // the connection that this prepared statement is bound to
	numInput int    // number of placeholder parameters
	withOpts map[string]string
}

var reWithOpt = regexp.MustCompile(`(?i)WITH\s+([\w-]+)\s*=\s*([\w/\.,;:'"-]+)`)

// parseWithOpts parses "WITH..." clause and store result in withOpts map.
// This function returns no error. Sub-implementations may override this behavior.
func (s *Stmt) parseWithOpts(withOptsStr string) error {
	s.withOpts = make(map[string]string)
	tokens := reWithOpt.FindAllStringSubmatch(withOptsStr, -1)
	for _, token := range tokens {
		s.withOpts[strings.TrimSpace(strings.ToUpper(token[1]))] = strings.TrimSpace(token[2])
	}
	return nil
}

// validateWithOpts is no-op in this struct. Sub-implementations may override this behavior.
func (s *Stmt) validateWithOpts() error {
	return nil
}

// NumInput implements driver.Stmt.Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt.NumInput.
func (s *Stmt) NumInput() int {
	return s.numInput
}

// // Exec implements driver.Stmt.Exec.
// func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
// 	panic("[Exec] implement me")
// }
//
// // Query implements driver.Stmt.Query.
// func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
// 	panic("[Query] implement me")
// }

func (s *Stmt) buildError(resp *gjrc.GjrcResponse) (error, int) {
	if resp.Error() != nil {
		return resp.Error(), 0
	}
	statusCode := resp.StatusCode()
	if statusCode >= 400 {
		body, _ := resp.Body()
		return fmt.Errorf("error executing Azure CosmosDB command; StatusCode=%d;Body=%s", resp.StatusCode(), body), statusCode
	}
	return nil, statusCode
}
