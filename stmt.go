package gocosmos

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
)

const (
	field       = `([\w\-]+)`
	ifNotExists = `(\s+IF\s+NOT\s+EXISTS)?`
	ifExists    = `(\s+IF\s+EXISTS)?`
	with        = `((\s+WITH\s+([\w-]+)\s*=\s*([\w/\.,;:'"-]+))*)`
)

var (
	reCreateDb = regexp.MustCompile(`(?is)^CREATE\s+DATABASE` + ifNotExists + `\s+` + field + with + `$`)
	reDropDb   = regexp.MustCompile(`(?is)^DROP\s+DATABASE` + ifExists + `\s+` + field + `$`)
	reListDbs  = regexp.MustCompile(`(?is)^LIST\s+DATABASES?$`)

	reCreateColl = regexp.MustCompile(`(?is)^CREATE\s+(COLLECTION|TABLE)` + ifNotExists + `\s+(` + field + `\.)?` + field + with + `$`)
	reDropColl   = regexp.MustCompile(`(?is)^DROP\s+(COLLECTION|TABLE)` + ifExists + `\s+(` + field + `\.)?` + field + `$`)
	reListColls  = regexp.MustCompile(`(?is)^LIST\s+(COLLECTIONS?|TABLES?)(\s+FROM\s+` + field + `)?$`)

	reInsert = regexp.MustCompile(`(?is)^(INSERT|UPSERT)\s+INTO\s+(` + field + `\.)?` + field + `\s*\(([^)]*?)\)\s*VALUES\s*\(([^)]*?)\)$`)
	reSelect = regexp.MustCompile(`(?is)^SELECT\s+(CROSS\s+PARTITION\s+)?.*?\s+FROM\s+` + field + `.*?` + with + `$`)
	reUpdate = regexp.MustCompile(`(?is)^UPDATE\s+(` + field + `\.)?` + field + `\s+SET\s+(.*)\s+WHERE\s+id\s*=\s*(.*)$`)
	reDelete = regexp.MustCompile(`(?is)^DELETE\s+FROM\s+(` + field + `\.)?` + field + `\s+WHERE\s+id\s*=\s*(.*)$`)
)

func parseQuery(c *Conn, query string) (driver.Stmt, error) {
	return parseQueryWithDefaultDb(c, "", query)
}

func parseQueryWithDefaultDb(c *Conn, defaultDb, query string) (driver.Stmt, error) {
	query = strings.TrimSpace(query)
	if re := reCreateDb; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateDatabase{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			dbName:      strings.TrimSpace(groups[0][2]),
			ifNotExists: strings.TrimSpace(groups[0][1]) != "",
			withOptsStr: strings.TrimSpace(groups[0][3]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDropDb; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropDatabase{
			Stmt:     &Stmt{query: query, conn: c, numInput: 0},
			dbName:   strings.TrimSpace(groups[0][2]),
			ifExists: strings.TrimSpace(groups[0][1]) != "",
		}
		return stmt, stmt.validate()
	}
	if re := reListDbs; re.MatchString(query) {
		stmt := &StmtListDatabases{
			Stmt: &Stmt{query: query, conn: c, numInput: 0},
		}
		return stmt, stmt.validate()
	}

	if re := reCreateColl; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateCollection{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			ifNotExists: strings.TrimSpace(groups[0][2]) != "",
			dbName:      strings.TrimSpace(groups[0][4]),
			collName:    strings.TrimSpace(groups[0][5]),
			withOptsStr: strings.TrimSpace(groups[0][6]),
		}
		if stmt.dbName == "" {
			stmt.dbName = defaultDb
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDropColl; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropCollection{
			Stmt:     &Stmt{query: query, conn: c, numInput: 0},
			dbName:   strings.TrimSpace(groups[0][4]),
			collName: strings.TrimSpace(groups[0][5]),
			ifExists: strings.TrimSpace(groups[0][2]) != "",
		}
		if stmt.dbName == "" {
			stmt.dbName = defaultDb
		}
		return stmt, stmt.validate()
	}
	if re := reListColls; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtListCollections{
			Stmt:   &Stmt{query: query, conn: c, numInput: 0},
			dbName: strings.TrimSpace(groups[0][3]),
		}
		if stmt.dbName == "" {
			stmt.dbName = defaultDb
		}
		return stmt, stmt.validate()
	}

	if re := reInsert; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtInsert{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			isUpsert:  strings.ToUpper(strings.TrimSpace(groups[0][1])) == "UPSERT",
			dbName:    strings.TrimSpace(groups[0][3]),
			collName:  strings.TrimSpace(groups[0][4]),
			fieldsStr: strings.TrimSpace(groups[0][5]),
			valuesStr: strings.TrimSpace(groups[0][6]),
		}
		if stmt.dbName == "" {
			stmt.dbName = defaultDb
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reSelect; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtSelect{
			Stmt:             &Stmt{query: query, conn: c, numInput: 0},
			isCrossPartition: strings.TrimSpace(groups[0][1]) != "",
			collName:         strings.TrimSpace(groups[0][2]),
			dbName:           defaultDb,
			selectQuery:      strings.ReplaceAll(strings.ReplaceAll(query, groups[0][1], ""), groups[0][3], ""),
		}
		if err := stmt.parse(groups[0][3]); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reUpdate; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtUpdate{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			dbName:    strings.TrimSpace(groups[0][2]),
			collName:  strings.TrimSpace(groups[0][3]),
			updateStr: strings.TrimSpace(groups[0][4]),
			idStr:     strings.TrimSpace(groups[0][5]),
		}
		if stmt.dbName == "" {
			stmt.dbName = defaultDb
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
			dbName:   strings.TrimSpace(groups[0][2]),
			collName: strings.TrimSpace(groups[0][3]),
			idStr:    strings.TrimSpace(groups[0][4]),
		}
		if stmt.dbName == "" {
			stmt.dbName = defaultDb
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

// // validateWithOpts is no-op in this struct. Sub-implementations may override this behavior.
// func (s *Stmt) validateWithOpts() error {
// 	return nil
// }

// Close implements driver.Stmt.Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt.NumInput.
func (s *Stmt) NumInput() int {
	return s.numInput
}
