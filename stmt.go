package go_cosmos

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"

	"github.com/btnguyen2k/consu/gjrc"
)

var (
	reA = regexp.MustCompile(`@\d+`)
	reC = regexp.MustCompile(`:\d+`)
	reD = regexp.MustCompile(`\$\d+`)

	reCreateDb = regexp.MustCompile(`(?i)^CREATE\s+DATABASE(\s+IF\s+NOT\s+EXISTS)?\s+([\w\-]+)((\s+WITH\s+([\w-]+)\s*=\s*([\w-]+))*)$`)
	reDropDb   = regexp.MustCompile(`(?i)^DROP\s+DATABASE(\s+IF\s+EXISTS)?\s+([\w\-]+)$`)
	reListDbs  = regexp.MustCompile(`(?i)^LIST\s+DATABASES?$`)

	reCreateColl = regexp.MustCompile(`(?i)^CREATE\s+(COLLECTION|TABLE)(\s+IF\s+NOT\s+EXISTS)?\s+([\w\-]+)\.([\w\-]+)((\s+WITH\s+([\w-]+)\s*=\s*([\w-]+))*)$`)
	reListColls  = regexp.MustCompile(`(?i)^LIST\s+(COLLECTIONS?|TABLES?)\s+FROM\s+([\w\-]+)$`)
)

func parseQuery(c *Conn, query string) (driver.Stmt, error) {
	query = strings.TrimSpace(query)
	if reCreateDb.MatchString(query) {
		groups := reCreateDb.FindAllStringSubmatch(query, -1)
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
	if reDropDb.MatchString(query) {
		groups := reDropDb.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropDatabase{
			Stmt:     &Stmt{query: query, conn: c, numInput: 0},
			dbName:   strings.TrimSpace(groups[0][2]),
			ifExists: strings.TrimSpace(groups[0][1]) != "",
		}
		return stmt, stmt.validateWithOpts()
	}
	if reListDbs.MatchString(query) {
		stmt := &StmtListDatabases{
			Stmt: &Stmt{query: query, conn: c, numInput: 0},
		}
		return stmt, stmt.validateWithOpts()
	}

	if reCreateColl.MatchString(query) {
		groups := reListColls.FindAllStringSubmatch(query, -1)
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
	if reListColls.MatchString(query) {
		groups := reListColls.FindAllStringSubmatch(query, -1)
		stmt := &StmtListCollections{
			Stmt:   &Stmt{query: query, conn: c, numInput: 0},
			dbName: strings.TrimSpace(groups[0][2]),
		}
		return stmt, stmt.validateWithOpts()
	}

	numInput := 0
	for _, regExp := range []*regexp.Regexp{reA, reC, reD} {
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
	withOpts map[string]string
}

var reWithOpt = regexp.MustCompile(`(?i)WITH\s+([\w-]+)\s*=\s*([\w-]+)`)

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
