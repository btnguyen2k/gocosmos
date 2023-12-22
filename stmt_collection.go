package gocosmos

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// StmtCreateCollection implements "CREATE COLLECTION" statement.
//
// Syntax:
//
//	CREATE COLLECTION|TABLE [IF NOT EXISTS] [<db-name>.]<collection-name>
//	<WITH PK=partitionKey>
//	[[,] WITH RU|MAXRU=ru]
//	[[,] WITH UK=/path1:/path2,/path3;/path4]
//
// - ru: an integer specifying CosmosDB's collection throughput expressed in RU/s. Supply either RU or MAXRU, not both!
//
// - partitionKey is either single (single value of /path) or hierarchical, up to 3 path levels, levels are separated by commas, for example: /path1,/path2,/path3.
//
// - If "IF NOT EXISTS" is specified, Exec will silently swallow the error "409 Conflict".
//
// - Use UK to define unique keys. Each unique key consists a list of paths separated by comma (,). Unique keys are separated by colons (:) or semi-colons (;).
type StmtCreateCollection struct {
	*Stmt
	dbName      string
	collName    string // collection name
	ifNotExists bool
	ru, maxru   int
	pk          string     // partition key
	uk          [][]string // unique keys
}

func (s *StmtCreateCollection) parse(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	for k, v := range s.withOpts {
		switch k {
		case "PK", "LARGEPK":
			if s.pk != "" {
				return fmt.Errorf("only one of PK or LARGEPK must be specified")
			}
			s.pk = v
		case "RU":
			ru, err := strconv.ParseInt(v, 10, 64)
			if err != nil || ru < 0 {
				return fmt.Errorf("invalid RU value: %s", v)
			}
			s.ru = int(ru)
		case "MAXRU":
			maxru, err := strconv.ParseInt(v, 10, 64)
			if err != nil || maxru < 0 {
				return fmt.Errorf("invalid MAXRU value: %s", v)
			}
			s.maxru = int(maxru)
		case "UK":
			tokens := regexp.MustCompile(`[;:]+`).Split(v, -1)
			for _, token := range tokens {
				paths := regexp.MustCompile(`[,\s]+`).Split(token, -1)
				s.uk = append(s.uk, paths)
			}
		default:
			return fmt.Errorf("invalid query, parsing error at WITH %s=%s", k, v)
		}
	}

	return nil
}

func (s *StmtCreateCollection) validate() error {
	if s.pk == "" {
		return fmt.Errorf("missing PartitionKey value")
	}
	if s.ru > 0 && s.maxru > 0 {
		return errors.New("only one of RU or MAXRU should be specified")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateCollection) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

// Exec implements driver.Stmt/Exec.
func (s *StmtCreateCollection) Exec(_ []driver.Value) (driver.Result, error) {
	pkPaths := strings.Split(s.pk, ",")
	pkType := "Hash"
	if len(pkPaths) > 1 {
		pkType = "MultiHash"
	}
	spec := CollectionSpec{DbName: s.dbName, CollName: s.collName, Ru: s.ru, MaxRu: s.maxru,
		PartitionKeyInfo: map[string]interface{}{
			"paths":   pkPaths,
			"kind":    pkType,
			"version": 2,
		},
	}
	if len(s.uk) > 0 {
		uniqueKeys := make([]interface{}, 0)
		for _, uk := range s.uk {
			uniqueKeys = append(uniqueKeys, map[string][]string{"paths": uk})
		}
		spec.UniqueKeyPolicy = map[string]interface{}{"uniqueKeys": uniqueKeys}
	}

	restResult := s.conn.restClient.CreateCollection(spec)
	ignoreErrorCode := 0
	if s.ifNotExists {
		ignoreErrorCode = 409
	}
	result := buildResultNoResultSet(&restResult.RestResponse, true, restResult.Rid, ignoreErrorCode)
	return result, result.err
}

/*----------------------------------------------------------------------*/

// StmtAlterCollection implements "ALTER COLLECTION" statement.
//
// Syntax:
//
//	ALTER COLLECTION|TABLE [<db-name>.]<collection-name> WITH RU|MAXRU=<ru>
//
// - ru: an integer specifying CosmosDB's collection throughput expressed in RU/s. Supply either RU or MAXRU, not both!
//
// Available since v0.1.1
type StmtAlterCollection struct {
	*Stmt
	dbName    string
	collName  string // collection name
	ru, maxru int
}

func (s *StmtAlterCollection) parse(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	for k, v := range s.withOpts {
		switch k {
		case "RU":
			ru, err := strconv.ParseInt(v, 10, 64)
			if err != nil || ru < 0 {
				return fmt.Errorf("invalid RU value: %s", v)
			}
			s.ru = int(ru)
		case "MAXRU":
			maxru, err := strconv.ParseInt(v, 10, 64)
			if err != nil || maxru < 0 {
				return fmt.Errorf("invalid MAXRU value: %s", v)
			}
			s.maxru = int(maxru)
		default:
			return fmt.Errorf("invalid query, parsing error at WITH %s=%s", k, v)
		}
	}

	return nil
}

func (s *StmtAlterCollection) validate() error {
	if (s.ru <= 0 && s.maxru <= 0) || (s.ru > 0 && s.maxru > 0) {
		return errors.New("only one of RU or MAXRU should be specified")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtAlterCollection) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

// Exec implements driver.Stmt/Exec.
func (s *StmtAlterCollection) Exec(_ []driver.Value) (driver.Result, error) {
	getResult := s.conn.restClient.GetCollection(s.dbName, s.collName)
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

// StmtDropCollection implements "DROP COLLECTION" statement.
//
// Syntax:
//
//	DROP COLLECTION|TABLE [IF EXISTS] [<db-name>.]<collection-name>
//
// If "IF EXISTS" is specified, Exec will silently swallow the error "404 Not Found".
type StmtDropCollection struct {
	*Stmt
	dbName   string
	collName string
	ifExists bool
}

func (s *StmtDropCollection) validate() error {
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropCollection) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

// Exec implements driver.Stmt/Exec.
func (s *StmtDropCollection) Exec(_ []driver.Value) (driver.Result, error) {
	restResult := s.conn.restClient.DeleteCollection(s.dbName, s.collName)
	ignoreErrorCode := 0
	if s.ifExists {
		ignoreErrorCode = 404
	}
	result := buildResultNoResultSet(&restResult.RestResponse, false, "", ignoreErrorCode)
	return result, result.err
}

/*----------------------------------------------------------------------*/

// StmtListCollections implements "LIST DATABASES" statement.
//
// Syntax:
//
//	LIST COLLECTIONS|TABLES|COLLECTION|TABLE [FROM <db-name>]
type StmtListCollections struct {
	*Stmt
	dbName string
}

func (s *StmtListCollections) validate() error {
	if s.dbName == "" {
		return errors.New("database is missing")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
// This function is not implemented, use Query instead.
func (s *StmtListCollections) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, ErrExecNotSupported
}

// Query implements driver.Stmt/Query.
func (s *StmtListCollections) Query(_ []driver.Value) (driver.Rows, error) {
	restResult := s.conn.restClient.ListCollections(s.dbName)
	result := &ResultResultSet{
		err:        restResult.Error(),
		columnList: []string{"id", "indexingPolicy", "_rid", "_ts", "_self", "_etag", "_docs", "_sprocs", "_triggers", "_udfs", "_conflicts"},
	}
	if result.err == nil {
		result.count = len(restResult.Collections)
		result.rows = make([]DocInfo, result.count)
		for i, coll := range restResult.Collections {
			result.rows[i] = coll.toMap()
		}
	}
	switch restResult.StatusCode {
	case 403:
		result.err = ErrForbidden
	case 404:
		result.err = ErrNotFound
	}
	return result, result.err
}
