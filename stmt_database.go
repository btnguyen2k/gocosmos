package gocosmos

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
)

// StmtCreateDatabase implements "CREATE DATABASE" statement.
//
// Syntax:
//
//	CREATE DATABASE [IF NOT EXISTS] <db-name> [WITH RU|MAXRU=ru]
//
// - ru: an integer specifying CosmosDB's database throughput expressed in RU/s. Supply either RU or MAXRU, not both!
//
// - If "IF NOT EXISTS" is specified, Exec will silently swallow the error "409 Conflict".
type StmtCreateDatabase struct {
	*Stmt
	dbName      string
	ifNotExists bool
	ru, maxru   int
}

// String implements fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtCreateDatabase) String() string {
	return fmt.Sprintf(`StmtCreateDatabase{Stmt: %s, db: %q,  if_not_exists: %t, ru: %d, maxru: %d}`,
		s.Stmt, s.dbName, s.ifNotExists, s.ru, s.maxru)
}

func (s *StmtCreateDatabase) parse(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	for k, v := range s.withOpts {
		switch k {
		case "RU":
			ru, err := strconv.ParseInt(v, 10, 32)
			if err != nil || ru < 0 {
				return fmt.Errorf("invalid RU value: %s", v)
			}
			s.ru = int(ru)
		case "MAXRU":
			maxru, err := strconv.ParseInt(v, 10, 32)
			if err != nil || maxru < 0 {
				return fmt.Errorf("invalid RU value: %s", v)
			}
			s.maxru = int(maxru)
		default:
			return fmt.Errorf("invalid query, parsing error at WITH %s=%s", k, v)
		}
	}

	return nil
}

func (s *StmtCreateDatabase) validate() error {
	if s.ru > 0 && s.maxru > 0 {
		return errors.New("only one of RU or MAXRU should be specified")
	}
	return nil
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

// Exec implements driver.Stmt/Exec.
func (s *StmtCreateDatabase) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), _valuesToNamedValues(args))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v1.1.0
func (s *StmtCreateDatabase) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("expected 0 input value, got %d", len(args))
	}

	// TODO: pass ctx to REST API client
	restResult := s.conn.restClient.CreateDatabase(DatabaseSpec{Id: s.dbName, Ru: s.ru, MaxRu: s.maxru})
	ignoreErrorCode := 0
	if s.ifNotExists {
		ignoreErrorCode = 409
	}
	result := buildResultNoResultSet(&restResult.RestResponse, true, restResult.Rid, ignoreErrorCode)
	return result, result.err
}

/*----------------------------------------------------------------------*/

// StmtAlterDatabase implements "ALTER DATABASE" statement.
//
// Syntax:
//
//	ALTER DATABASE <db-name> WITH RU|MAXRU=<ru>
//
// - ru: an integer specifying CosmosDB's database throughput expressed in RU/s. Supply either RU or MAXRU, not both!
//
// Available since v0.1.1
type StmtAlterDatabase struct {
	*Stmt
	dbName    string
	ru, maxru int
}

// String implements fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtAlterDatabase) String() string {
	return fmt.Sprintf(`StmtAlterDatabase{Stmt: %s, db: %q, ru: %d, maxru: %d}`,
		s.Stmt, s.dbName, s.ru, s.maxru)
}

func (s *StmtAlterDatabase) parse(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	for k, v := range s.withOpts {
		switch k {
		case "RU":
			ru, err := strconv.ParseInt(v, 10, 32)
			if err != nil || ru < 0 {
				return fmt.Errorf("invalid RU value: %s", v)
			}
			s.ru = int(ru)
		case "MAXRU":
			maxru, err := strconv.ParseInt(v, 10, 32)
			if err != nil || maxru < 0 {
				return fmt.Errorf("invalid RU value: %s", v)
			}
			s.maxru = int(maxru)
		default:
			return fmt.Errorf("invalid query, parsing error at WITH %s=%s", k, v)
		}
	}

	return nil
}

func (s *StmtAlterDatabase) validate() error {
	if (s.ru <= 0 && s.maxru <= 0) || (s.ru > 0 && s.maxru > 0) {
		return errors.New("only one of RU or MAXRU must be specified")
	}
	return nil
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtAlterDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

// Exec implements driver.Stmt/Exec.
func (s *StmtAlterDatabase) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), _valuesToNamedValues(args))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v1.1.1
func (s *StmtAlterDatabase) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("expected 0 input value, got %d", len(args))
	}

	// TODO: pass ctx to REST API client
	getResult := s.conn.restClient.GetDatabase(s.dbName)
	if err := getResult.Error(); err != nil {
		switch getResult.StatusCode {
		case 403:
			return nil, ErrForbidden
		case 404:
			return nil, ErrNotFound
		}
		return nil, err
	}
	restResult := s.conn.restClient.ReplaceOfferForResource(getResult.Rid, s.ru, s.maxru)
	result := buildResultNoResultSet(&restResult.RestResponse, true, restResult.Rid, 0)
	return result, result.err
}

/*----------------------------------------------------------------------*/

// StmtDropDatabase implements "DROP DATABASE" statement.
//
// Syntax:
//
//	DROP DATABASE [IF EXISTS] <db-name>
//
// - If "IF EXISTS" is specified, Exec will silently swallow the error "404 Not Found".
type StmtDropDatabase struct {
	*Stmt
	dbName   string
	ifExists bool
}

// String implements fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtDropDatabase) String() string {
	return fmt.Sprintf(`StmtDropDatabase{Stmt: %s, db: %q, if_exists: %t}`,
		s.Stmt, s.dbName, s.ifExists)
}

func (s *StmtDropDatabase) validate() error {
	return nil
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropDatabase) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

// Exec implements driver.Stmt/Exec.
func (s *StmtDropDatabase) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), _valuesToNamedValues(args))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v1.1.0
func (s *StmtDropDatabase) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("expected 0 input value, got %d", len(args))
	}

	// TODO: pass ctx to REST API client
	restResult := s.conn.restClient.DeleteDatabase(s.dbName)
	ignoreErrorCode := 0
	if s.ifExists {
		ignoreErrorCode = 404
	}
	result := buildResultNoResultSet(&restResult.RestResponse, false, "", ignoreErrorCode)
	return result, result.err
}

/*----------------------------------------------------------------------*/

// StmtListDatabases implements "LIST DATABASES" statement.
//
// Syntax:
//
//	LIST DATABASES|DATABASE
type StmtListDatabases struct {
	*Stmt
}

// String implements fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtListDatabases) String() string {
	return fmt.Sprintf(`StmtListDatabases{Stmt: %s}`, s.Stmt)
}

func (s *StmtListDatabases) validate() error {
	return nil
}

// Exec implements driver.Stmt/Exec.
// This function is not implemented, use Query instead.
func (s *StmtListDatabases) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, ErrExecNotSupported
}

// Query implements driver.Stmt/Query.
func (s *StmtListDatabases) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), _valuesToNamedValues(args))
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
//
// @Available since v1.1.0
func (s *StmtListDatabases) QueryContext(_ context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("expected 0 input value, got %d", len(args))
	}

	// TODO: pass ctx to REST API client
	restResult := s.conn.restClient.ListDatabases()
	result := &ResultResultSet{
		err:        restResult.Error(),
		columnList: []string{"id", "_rid", "_ts", "_self", "_etag", "_colls", "_users"},
	}
	if result.err == nil {
		result.count = len(restResult.Databases)
		result.rows = make([]DocInfo, result.count)
		for i, db := range restResult.Databases {
			result.rows[i] = db.toMap()
		}
	}
	switch restResult.StatusCode {
	case 403:
		result.err = ErrForbidden
	}
	return result, result.err
}
