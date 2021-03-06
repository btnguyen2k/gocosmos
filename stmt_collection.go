package gocosmos

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
)

// StmtCreateCollection implements "CREATE COLLECTION" operation.
//
// Syntax:
//     CREATE COLLECTION|TABLE [IF NOT EXISTS] [<db-name>.]<collection-name> <WITH [LARGE]PK=partitionKey> [WITH RU|MAXRU=ru] [WITH UK=/path1:/path2,/path3;/path4]
//
// - ru: an integer specifying CosmosDB's collection throughput expressed in RU/s. Supply either RU or MAXRU, not both!
//
// - If "IF NOT EXISTS" is specified, Exec will silently swallow the error "409 Conflict".
//
// - Use LARGEPK if partitionKey is larger than 100 bytes.
//
// - Use UK to define unique keys. Each unique key consists a list of paths separated by comma (,). Unique keys are separated by colons (:) or semi-colons (;).
type StmtCreateCollection struct {
	*Stmt
	dbName      string
	collName    string // collection name
	ifNotExists bool
	isLargePk   bool
	ru, maxru   int
	pk          string     // partition key
	uk          [][]string // unique keys
	withOptsStr string
}

func (s *StmtCreateCollection) parse() error {
	if err := s.Stmt.parseWithOpts(s.withOptsStr); err != nil {
		return err
	}

	// partition key
	pk, okPk := s.withOpts["PK"]
	largePk, okLargePk := s.withOpts["LARGEPK"]
	if pk != "" && largePk != "" {
		return fmt.Errorf("only one of PK or LARGEPK must be specified")
	}
	if !okPk && !okLargePk && pk == "" && largePk == "" {
		return fmt.Errorf("invalid or missting PartitionKey value: %s%s", s.withOpts["PK"], s.withOpts["LARGEPK"])
	}
	if okPk && pk != "" {
		s.pk = pk
	}
	if okLargePk && largePk != "" {
		s.pk = largePk
		s.isLargePk = true
	}

	// request unit
	if _, ok := s.withOpts["RU"]; ok {
		ru, err := strconv.ParseInt(s.withOpts["RU"], 10, 64)
		if err != nil || ru < 0 {
			return fmt.Errorf("invalid RU value: %s", s.withOpts["RU"])
		}
		s.ru = int(ru)
	}
	if _, ok := s.withOpts["MAXRU"]; ok {
		maxru, err := strconv.ParseInt(s.withOpts["MAXRU"], 10, 64)
		if err != nil || maxru < 0 {
			return fmt.Errorf("invalid MAXRU value: %s", s.withOpts["MAXRU"])
		}
		s.maxru = int(maxru)
	}

	// unique key
	if ukOpts, ok := s.withOpts["UK"]; ok && ukOpts != "" {
		tokens := regexp.MustCompile(`[;:]+`).Split(ukOpts, -1)
		for _, token := range tokens {
			paths := regexp.MustCompile(`[,\s]+`).Split(token, -1)
			s.uk = append(s.uk, paths)
		}
	}

	return nil
}

func (s *StmtCreateCollection) validate() error {
	if s.ru > 0 && s.maxru > 0 {
		return errors.New("only one of RU or MAXRU must be specified")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateCollection) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
// Upon successful call, this function returns (*ResultCreateCollection, nil).
func (s *StmtCreateCollection) Exec(_ []driver.Value) (driver.Result, error) {
	spec := CollectionSpec{DbName: s.dbName, CollName: s.collName, Ru: s.ru, MaxRu: s.maxru,
		PartitionKeyInfo: map[string]interface{}{
			"paths": []string{s.pk},
			"kind":  "Hash",
		}}
	if s.isLargePk {
		spec.PartitionKeyInfo["Version"] = 2
	}
	if len(s.uk) > 0 {
		uniqueKeys := make([]interface{}, 0)
		for _, uk := range s.uk {
			uniqueKeys = append(uniqueKeys, map[string][]string{"paths": uk})
		}
		spec.UniqueKeyPolicy = map[string]interface{}{"uniqueKeys": uniqueKeys}
	}

	restResult := s.conn.restClient.CreateCollection(spec)
	result := &ResultCreateCollection{Successful: restResult.Error() == nil, InsertId: restResult.Rid}
	err := restResult.Error()
	switch restResult.StatusCode {
	case 403:
		err = ErrForbidden
	case 404:
		err = ErrNotFound
	case 409:
		if s.ifNotExists {
			err = nil
		} else {
			err = ErrConflict
		}
	}
	return result, err
}

// ResultCreateCollection captures the result from CREATE COLLECTION operation.
type ResultCreateCollection struct {
	// Successful flags if the operation was successful or not.
	Successful bool
	// InsertId holds the "_rid" if the operation was successful.
	InsertId string
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultCreateCollection) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported. {LastInsertId:%s}", r.InsertId)
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultCreateCollection) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtAlterCollection implements "ALTER COLLECTION" operation.
//
// Syntax:
//     ALTER COLLECTION|TABLE [<db-name>.]<collection-name> WITH RU|MAXRU=<ru>
//
// - ru: an integer specifying CosmosDB's collection throughput expressed in RU/s. Supply either RU or MAXRU, not both!
//
// Available since v0.1.1
type StmtAlterCollection struct {
	*Stmt
	dbName      string
	collName    string // collection name
	ru, maxru   int
	withOptsStr string
}

func (s *StmtAlterCollection) parse() error {
	if err := s.Stmt.parseWithOpts(s.withOptsStr); err != nil {
		return err
	}

	if _, ok := s.withOpts["RU"]; ok {
		ru, err := strconv.ParseInt(s.withOpts["RU"], 10, 64)
		if err != nil || ru < 0 {
			return fmt.Errorf("invalid RU value: %s", s.withOpts["RU"])
		}
		s.ru = int(ru)
	}
	if _, ok := s.withOpts["MAXRU"]; ok {
		maxru, err := strconv.ParseInt(s.withOpts["MAXRU"], 10, 64)
		if err != nil || maxru < 0 {
			return fmt.Errorf("invalid MAXRU value: %s", s.withOpts["MAXRU"])
		}
		s.maxru = int(maxru)
	}

	return nil
}

func (s *StmtAlterCollection) validate() error {
	if (s.ru <= 0 && s.maxru <= 0) || (s.ru > 0 && s.maxru > 0) {
		return errors.New("only one of RU or MAXRU must be specified")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtAlterCollection) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
// Upon successful call, this function returns (*ResultAlterCollection, nil).
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
	result := &ResultAlterCollection{Successful: restResult.Error() == nil}
	err := restResult.Error()
	switch restResult.StatusCode {
	case 403:
		err = ErrForbidden
	case 404:
		err = ErrNotFound
	}
	return result, err
}

// ResultAlterCollection captures the result from ALTER COLLECTION operation.
//
// Available since v0.1.1
type ResultAlterCollection struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultAlterCollection) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultAlterCollection) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDropCollection implements "DROP COLLECTION" operation.
//
// Syntax:
//     DROP COLLECTION|TABLE [IF EXISTS] [<db-name>.]<collection-name>
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

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropCollection) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use exec")
}

// Exec implements driver.Stmt.Exec.
// This function always return a nil driver.Result.
func (s *StmtDropCollection) Exec(_ []driver.Value) (driver.Result, error) {
	restResult := s.conn.restClient.DeleteCollection(s.dbName, s.collName)
	err := restResult.Error()
	switch restResult.StatusCode {
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

// StmtListCollections implements "LIST DATABASES" operation.
//
// Syntax:
//     LIST COLLECTIONS|TABLES|COLLECTION|TABLE [FROM <db-name>]
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

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtListCollections) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use query")
}

// Query implements driver.Stmt.Query.
func (s *StmtListCollections) Query(_ []driver.Value) (driver.Rows, error) {
	restResult := s.conn.restClient.ListCollections(s.dbName)
	err := restResult.Error()
	var rows driver.Rows
	if err == nil {
		rows = &RowsListCollections{
			count:       int(restResult.Count),
			collections: restResult.Collections,
			cursorCount: 0,
		}
	}
	switch restResult.StatusCode {
	case 403:
		err = ErrForbidden
	case 404:
		err = ErrNotFound
	}
	return rows, err
}

// RowsListCollections captures the result from LIST COLLECTIONS operation.
type RowsListCollections struct {
	count       int
	collections []CollInfo
	cursorCount int
}

// Columns implements driver.Rows.Columns.
func (r *RowsListCollections) Columns() []string {
	return []string{"id", "indexingPolicy", "_rid", "_ts", "_self", "_etag", "_docs", "_sprocs", "_triggers", "_udfs", "_conflicts"}
}

// Close implements driver.Rows.Close.
func (r *RowsListCollections) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *RowsListCollections) Next(dest []driver.Value) error {
	if r.cursorCount >= r.count {
		return io.EOF
	}
	rowData := r.collections[r.cursorCount]
	r.cursorCount++
	dest[0] = rowData.Id
	dest[1] = rowData.IndexingPolicy
	dest[2] = rowData.Rid
	dest[3] = rowData.Ts
	dest[4] = rowData.Self
	dest[5] = rowData.Etag
	dest[6] = rowData.Docs
	dest[7] = rowData.Sprocs
	dest[8] = rowData.Triggers
	dest[9] = rowData.Udfs
	dest[10] = rowData.Conflicts
	return nil
}
