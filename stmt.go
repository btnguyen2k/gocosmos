package gocosmos

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

const (
	field       = `([\w\-]+)`
	ifNotExists = `(\s+IF\s+NOT\s+EXISTS)?`
	ifExists    = `(\s+IF\s+EXISTS)?`
	with        = `(\s+WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+)((\s+|\s*,\s+|\s+,\s*)WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+))*)?`
	// with        = `((\s+WITH\s+([\w-]+)\s*=\s*([\w/\.,;:'"-]+))*)`
)

var (
	reCreateDb = regexp.MustCompile(`(?is)^CREATE\s+DATABASE` + ifNotExists + `\s+` + field + with + `$`)
	reAlterDb  = regexp.MustCompile(`(?is)^ALTER\s+DATABASE` + `\s+` + field + with + `$`)
	reDropDb   = regexp.MustCompile(`(?is)^DROP\s+DATABASE` + ifExists + `\s+` + field + `$`)
	reListDbs  = regexp.MustCompile(`(?is)^LIST\s+DATABASES?$`)

	reCreateColl = regexp.MustCompile(`(?is)^CREATE\s+(COLLECTION|TABLE)` + ifNotExists + `\s+(` + field + `\.)?` + field + with + `$`)
	reAlterColl  = regexp.MustCompile(`(?is)^ALTER\s+(COLLECTION|TABLE)` + `\s+(` + field + `\.)?` + field + with + `$`)
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
	if re := reAlterDb; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtAlterDatabase{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			dbName:      strings.TrimSpace(groups[0][1]),
			withOptsStr: strings.TrimSpace(groups[0][2]),
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
	if re := reAlterColl; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtAlterCollection{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			dbName:      strings.TrimSpace(groups[0][3]),
			collName:    strings.TrimSpace(groups[0][4]),
			withOptsStr: strings.TrimSpace(groups[0][5]),
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

// Close implements driver.Stmt/Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt/NumInput.
func (s *Stmt) NumInput() int {
	return s.numInput
}

/*----------------------------------------------------------------------*/

func buildResultNoResultSet(restResponse *RestReponse, supportLastInsertId bool, rid string, ignoreErrorCode int) *ResultNoResultSet {
	result := &ResultNoResultSet{
		err:                 restResponse.Error(),
		lastInsertId:        rid,
		supportLastInsertId: supportLastInsertId,
	}
	if result.err == nil {
		result.affectedRows = 1
	}
	switch restResponse.StatusCode {
	case 403:
		if ignoreErrorCode == 403 {
			result.err = nil
		} else {
			result.err = ErrForbidden
		}
	case 404:
		if ignoreErrorCode == 404 {
			result.err = nil
		} else {
			result.err = ErrNotFound
		}
	case 409:
		if ignoreErrorCode == 409 {
			result.err = nil
		} else {
			result.err = ErrConflict
		}
	}
	return result
}

// ResultNoResultSet captures the result from statements that do not expect a ResultSet to be returned.
//
// @Available since v0.3.0
type ResultNoResultSet struct {
	err                 error
	affectedRows        int64
	supportLastInsertId bool
	lastInsertId        string // holds the "_rid" if the operation returns it
}

// LastInsertId implements driver.Result/LastInsertId.
func (r *ResultNoResultSet) LastInsertId() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if !r.supportLastInsertId {
		return 0, ErrOperationNotSupported
	}
	return 0, fmt.Errorf(`{"last_insert_id":"%s"}`, r.lastInsertId)
}

// RowsAffected implements driver.Result/RowsAffected.
func (r *ResultNoResultSet) RowsAffected() (int64, error) {
	return r.affectedRows, r.err
}

/*----------------------------------------------------------------------*/

// ResultResultSet captures the result from statements that expect a ResultSet to be returned.
//
// @Available since v0.3.0
type ResultResultSet struct {
	err         error
	count       int
	cursorCount int
	columnList  []string
	columnTypes map[string]reflect.Type
	rowData     []map[string]interface{}
}

func (r *ResultResultSet) init() *ResultResultSet {
	if r.rowData == nil {
		return r
	}
	if r.columnTypes == nil {
		r.columnTypes = make(map[string]reflect.Type)
	}
	r.count = len(r.rowData)
	colMap := make(map[string]bool)
	for _, item := range r.rowData {
		for col, val := range item {
			colMap[col] = true
			if r.columnTypes[col] == nil {
				r.columnTypes[col] = reflect.TypeOf(val)
			}
		}
	}
	r.columnList = make([]string, 0, len(colMap))
	for col := range colMap {
		r.columnList = append(r.columnList, col)
	}
	sort.Strings(r.columnList)

	return r
}

// Columns implements driver.Rows/Columns.
func (r *ResultResultSet) Columns() []string {
	return r.columnList
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType/ColumnTypeScanType
func (r *ResultResultSet) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypes[r.columnList[index]]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName/ColumnTypeDatabaseTypeName
func (r *ResultResultSet) ColumnTypeDatabaseTypeName(index int) string {
	return goTypeToCosmosDbType(r.columnTypes[r.columnList[index]])
}

// Close implements driver.Rows/Close.
func (r *ResultResultSet) Close() error {
	return r.err
}

// Next implements driver.Rows/Next.
func (r *ResultResultSet) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	if r.cursorCount >= r.count {
		return io.EOF
	}
	rowData := r.rowData[r.cursorCount]
	r.cursorCount++
	for i, colName := range r.columnList {
		dest[i] = rowData[colName]
	}
	return nil
}
