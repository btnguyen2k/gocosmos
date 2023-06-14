package gocosmos

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

func _parseValue(input string, separator rune) (value interface{}, leftOver string, err error) {
	if loc := reValPlaceholder.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimFunc(input[loc[0]+1:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		index, err := strconv.Atoi(token)
		return placeholder{index}, input[loc[1]:], err
	}
	if loc := reValNull.FindStringIndex(input); loc != nil && loc[0] == 0 {
		return nil, input[loc[1]:], nil
	}
	if loc := reValNumber.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		var data interface{}
		err := json.Unmarshal([]byte(token), &data)
		if err != nil {
			err = errors.New("(nul) cannot parse query, invalid token at: " + token)
		}
		return data, input[loc[1]:], err
	}
	if loc := reValBoolean.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		var data bool
		err := json.Unmarshal([]byte(token), &data)
		// if err != nil {
		// 	err = errors.New("(bool) cannot parse query, invalid token at: " + token)
		// }
		return data, input[loc[1]:], err
	}
	if loc := reValString.FindStringIndex(input); loc != nil && loc[0] == 0 {
		var data interface{}
		token, err := strconv.Unquote(strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator }))
		if err == nil {
			err = json.Unmarshal([]byte(token), &data)
			if err != nil {
				err = errors.New("(unmarshal) cannot parse query, invalid token at: " + token)
			}
		} else {
			err = errors.New("(unquote) cannot parse query, invalid token at: " + token)
		}
		return data, input[loc[1]:], err
	}
	return nil, input, errors.New("cannot parse query, invalid token at: " + input)
}

// StmtCRUD is abstract implementation of "INSERT|UPSERT|UPDATE|DELETE|SELECT" operations.
//
// @Available since v0.3.0
type StmtCRUD struct {
	*Stmt
	dbName         string
	collName       string
	numPkPaths     int // number of PK paths
	isSinglePathPk bool
}

func (s *StmtCRUD) extractPkValuesFromArgs(args ...driver.Value) []interface{} {
	n := len(args)
	result := make([]interface{}, s.numPkPaths)
	for i := n - s.numPkPaths; i < n; i++ {
		result[i-n+s.numPkPaths] = args[i]
	}
	return result
}

func (s *StmtCRUD) fetchPkInfo() error {
	if s.numPkPaths > 0 || s.conn == nil || s.isSinglePathPk {
		if s.isSinglePathPk {
			s.numPkPaths = 1
		}
		return nil
	}

	getCollResult := s.conn.restClient.GetCollection(s.dbName, s.collName)
	if getCollResult.Error() == nil {
		s.numPkPaths = len(getCollResult.CollInfo.PartitionKey.Paths())
	}
	return normalizeError(getCollResult.StatusCode, 0, getCollResult.Error())
}

func (s *StmtCRUD) parseWithOpts(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}
	_, ok1 := s.withOpts["SINGLEPK"]
	_, ok2 := s.withOpts["SINGLE_PK"]
	s.isSinglePathPk = ok1 || ok2
	if s.isSinglePathPk {
		s.numPkPaths = 1
	}
	s.numInput = 0
	return nil
}

// StmtInsert implements "INSERT" operation.
//
// Syntax:
//
//	INSERT|UPSERT INTO <db-name>.<collection-name> (<field-list>) VALUES (<value-list>) [WITH singlePK|SINGLE_PK]
//
//	- values are comma separated.
//	- a value is either:
//	  - a placeholder (e.g. :1, @2 or $3)
//	  - a null
//	  - a number
//	  - a boolean (true/false)
//	  - a string (inside double quotes) that must be a valid JSON, e.g.
//	    - a string value in JSON (include the double quotes): "\"a string\""
//	    - a number value in JSON (include the double quotes): "123"
//	    - a boolean value in JSON (include the double quotes): "true"
//	    - a null value in JSON (include the double quotes): "null"
//	    - a map value in JSON (include the double quotes): "{\"key\":\"value\"}"
//	    - a list value in JSON (include the double quotes): "[1,true,null,\"string\"]"
//
// CosmosDB automatically creates a few extra fields for the insert document.
// See https://docs.microsoft.com/en-us/azure/cosmos-db/account-databases-containers-items#properties-of-an-item.
type StmtInsert struct {
	*StmtCRUD
	isUpsert  bool
	fieldsStr string
	valuesStr string
	fields    []string
	values    []interface{}
}

func (s *StmtInsert) parse(withOptsStr string) error {
	if err := s.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	s.fields = regexp.MustCompile(`[,\s]+`).Split(s.fieldsStr, -1)
	s.values = make([]interface{}, 0)
	for temp := strings.TrimSpace(s.valuesStr); temp != ""; temp = strings.TrimSpace(temp) {
		value, leftOver, err := _parseValue(temp, ',')
		if err == nil {
			s.values = append(s.values, value)
			temp = leftOver
			switch value.(type) {
			case placeholder:
				s.numInput++
			}
			continue
		}
		return err
	}
	return nil
}

func (s *StmtInsert) validate() error {
	if len(s.fields) != len(s.values) {
		return fmt.Errorf("number of field (%d) does not match number of input value (%d)", len(s.fields), len(s.values))
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
//
// Note: this function expects the _partition key values are placed at the end_ of the argument list.
func (s *StmtInsert) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.fetchPkInfo(); err != nil {
		return nil, err
	}
	if len(args) != s.numInput+s.numPkPaths {
		return nil, fmt.Errorf("expected %d arguments, got %d", s.numInput+s.numPkPaths, len(args))
	}

	spec := DocumentSpec{
		DbName:             s.dbName,
		CollName:           s.collName,
		IsUpsert:           s.isUpsert,
		PartitionKeyValues: s.extractPkValuesFromArgs(args...),
		DocumentData:       make(map[string]interface{}),
	}
	for i := 0; i < len(s.fields); i++ {
		switch s.values[i].(type) {
		case placeholder:
			ph := s.values[i].(placeholder)
			if ph.index <= 0 || ph.index >= len(args) {
				return nil, fmt.Errorf("invalid value index %d", ph.index)
			}
			spec.DocumentData[s.fields[i]] = args[ph.index-1]
		default:
			spec.DocumentData[s.fields[i]] = s.values[i]
		}
	}
	restResult := s.conn.restClient.CreateDocument(spec)
	rid := ""
	if restResult.DocInfo != nil {
		rid, _ = restResult.DocInfo["_rid"].(string)
	}
	result := buildResultNoResultSet(&restResult.RestReponse, true, rid, 0)
	return result, result.err
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtInsert) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

/*----------------------------------------------------------------------*/

// StmtDelete implements "DELETE" operation.
//
// Syntax:
//
//	DELETE FROM <db-name>.<collection-name> WHERE id=<id-value> [WITH singlePK|SINGLE_PK]
//
// - Currently DELETE only removes one document specified by id.
//
// - <id-value> is treated as string. `WHERE id=abc` has the same effect as `WHERE id="abc"`.
type StmtDelete struct {
	*StmtCRUD
	idStr string
	id    interface{}
}

func (s *StmtDelete) parse(withOptsStr string) error {
	if err := s.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	hasPrefix := strings.HasPrefix(s.idStr, `"`)
	hasSuffix := strings.HasSuffix(s.idStr, `"`)
	if hasPrefix != hasSuffix {
		return fmt.Errorf("invalid id literate: %s", s.idStr)
	}
	if hasPrefix && hasSuffix {
		s.idStr = strings.TrimSpace(s.idStr[1 : len(s.idStr)-1])
	} else if loc := reValPlaceholder.FindStringIndex(s.idStr); loc != nil {
		if loc[0] == 0 && loc[1] == len(s.idStr) {
			index, _ := strconv.Atoi(s.idStr[loc[0]+1:])
			s.id = placeholder{index}
			s.numInput++
		} else {
			return fmt.Errorf("invalid id literate: %s", s.idStr)
		}
	}
	return nil
}

func (s *StmtDelete) validate() error {
	if s.idStr == "" {
		return errors.New("id value is missing")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
//
// Note: this function expects the _partition key values are placed at the end_ of the argument list.
func (s *StmtDelete) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.fetchPkInfo(); err != nil {
		return nil, err
	}
	if len(args) != s.numInput+s.numPkPaths {
		return nil, fmt.Errorf("expected %d arguments, got %d", s.numInput+s.numPkPaths, len(args))
	}

	id := s.idStr
	if s.id != nil {
		ph := s.id.(placeholder)
		if ph.index <= 0 || ph.index >= len(args) {
			return nil, fmt.Errorf("invalid value index %d", ph.index)
		}
		id = fmt.Sprintf("%s", args[ph.index-1])
	}
	restResult := s.conn.restClient.DeleteDocument(DocReq{DbName: s.dbName, CollName: s.collName, DocId: id,
		PartitionKeyValues: s.extractPkValuesFromArgs(args...),
	})
	result := buildResultNoResultSet(&restResult.RestReponse, false, "", 0)
	switch restResult.StatusCode {
	case 404:
		// consider "document not found" as successful operation
		// but database/collection not found is not!
		if strings.Index(fmt.Sprintf("%s", restResult.Error()), "ResourceType: Document") >= 0 {
			result.err = nil
		}
	}
	return result, result.err
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtDelete) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}

/*----------------------------------------------------------------------*/

// StmtSelect implements "SELECT" operation.
// The "SELECT" query follows CosmosDB's SQL grammar (https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-select) with a few extensions:
//
// Syntax:
//
//	SELECT [CROSS PARTITION] ... FROM <collection/table-name> ...
//	WITH database|db=<db-name>
//	[WITH collection|table=<collection/table-name>]
//	[WITH cross_partition=true]
//
//	- (extension) If the collection is partitioned, specify "CROSS PARTITION" to allow execution across multiple partitions.
//	  This clause is not required if query is to be executed on a single partition.
//	  Cross-partition execution can also be enabled using WITH cross_partition=true.
//	- (extension) Use "WITH database=<db-name>" (or "WITH db=<db-name>") to specify the database on which the query is to be executed.
//	- (extension) Use "WITH collection=<coll-name>" (or "WITH table=<coll-name>") to specify the collection/table on which the query is to be executed.
//	  If not specified, collection/table name is extracted from the "FROM <collection/table-name>" clause.
//	- (extension) Use placeholder syntax @i, $i or :i (where i denotes the i-th parameter, the first parameter is 1)
type StmtSelect struct {
	*Stmt
	isCrossPartition bool
	dbName           string
	collName         string
	selectQuery      string
	placeholders     map[int]string
}

func (s *StmtSelect) parse(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}
	if v, ok := s.withOpts["DATABASE"]; ok {
		s.dbName = strings.TrimSpace(v)
	} else if v, ok := s.withOpts["DB"]; ok {
		s.dbName = strings.TrimSpace(v)
	}
	if v, ok := s.withOpts["COLLECTION"]; ok {
		s.collName = strings.TrimSpace(v)
	} else if v, ok := s.withOpts["TABLE"]; ok {
		s.collName = strings.TrimSpace(v)
	}
	if v, ok := s.withOpts["CROSS_PARTITION"]; ok && !s.isCrossPartition {
		vbool, err := strconv.ParseBool(v)
		if err != nil || !vbool {
			return errors.New("cannot parse query (the only accepted value for cross_partition is true), invalid token at: " + v)
		}
		s.isCrossPartition = true
	}

	matches := reValPlaceholder.FindAllStringSubmatch(s.selectQuery, -1)
	s.numInput = len(matches)
	s.placeholders = make(map[int]string)
	for _, match := range matches {
		v, _ := strconv.Atoi(match[1])
		key := "@_" + match[1]
		s.placeholders[v] = key
		if strings.HasSuffix(match[0], " ") {
			key += " "
		}
		s.selectQuery = strings.ReplaceAll(s.selectQuery, match[0], key)
	}

	return nil
}

func (s *StmtSelect) validate() error {
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	return nil
}

// Query implements driver.Stmt/Query.
func (s *StmtSelect) Query(args []driver.Value) (driver.Rows, error) {
	params := make([]interface{}, 0)
	for i, arg := range args {
		v, ok := s.placeholders[i+1]
		if !ok {
			return nil, fmt.Errorf("there is no placeholder #%d", i+1)
		}
		params = append(params, map[string]interface{}{"name": fmt.Sprintf("%s", v), "value": arg})
	}
	query := QueryReq{
		DbName:                s.dbName,
		CollName:              s.collName,
		Query:                 s.selectQuery,
		Params:                params,
		CrossPartitionEnabled: s.isCrossPartition,
	}

	restResult := s.conn.restClient.QueryDocumentsCrossPartition(query)
	result := &ResultResultSet{err: restResult.Error(), columnList: make([]string, 0)}
	if result.err == nil {
		result.documents = restResult.Documents
		result.init()
	}
	result.err = normalizeError(restResult.StatusCode, 0, result.err)
	return result, result.err
}

// Exec implements driver.Stmt/Exec.
// This function is not implemented, use Query instead.
func (s *StmtSelect) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, ErrExecNotSupported
}

/*----------------------------------------------------------------------*/

// StmtUpdate implements "UPDATE" operation.
//
// Syntax:
//
//	UPDATE <db-name>.<collection-name> SET <field-name1>=<value1>[,<field-nameN>=<valueN>]* WHERE id=<id-value> [WITH singlePK|SINGLE_PK]
//
//	- <id-value> is treated as a string. `WHERE id=abc` has the same effect as `WHERE id="abc"`.
//	- <value> is either:
//	  - a placeholder (e.g. :1, @2 or $3)
//	  - a null
//	  - a number
//	  - a boolean (true/false)
//	  - a string (inside double quotes) that must be a valid JSON, e.g.
//	    - a string value in JSON (include the double quotes): "\"a string\""
//	    - a number value in JSON (include the double quotes): "123"
//	    - a boolean value in JSON (include the double quotes): "true"
//	    - a null value in JSON (include the double quotes): "null"
//	    - a map value in JSON (include the double quotes): "{\"key\":\"value\"}"
//	    - a list value in JSON (include the double quotes): "[1,true,null,\"string\"]"
//
// Currently UPDATE only updates one document specified by id.
type StmtUpdate struct {
	*StmtCRUD
	updateStr string
	idStr     string
	id        interface{}
	fields    []string
	values    []interface{}
}

func (s *StmtUpdate) _parseId() error {
	hasPrefix := strings.HasPrefix(s.idStr, `"`)
	hasSuffix := strings.HasSuffix(s.idStr, `"`)
	if hasPrefix != hasSuffix {
		return fmt.Errorf("invalid id literate: %s", s.idStr)
	}
	if hasPrefix && hasSuffix {
		s.idStr = strings.TrimSpace(s.idStr[1 : len(s.idStr)-1])
	} else if loc := reValPlaceholder.FindStringIndex(s.idStr); loc != nil {
		index, _ := strconv.Atoi(s.idStr[loc[0]+1:])
		s.id = placeholder{index}
		s.numInput++
	}
	return nil
}

var (
	reFieldPart = regexp.MustCompile(`\s*([\w\-]+)\s*=`)
)

func _isSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '=':
		return true
	}
	return false
}

func (s *StmtUpdate) _parseUpdateClause() error {
	s.fields = make([]string, 0)
	s.values = make([]interface{}, 0)
	for temp := strings.TrimSpace(s.updateStr); temp != ""; temp = strings.TrimSpace(temp) {
		// firstly, extract the field name
		if loc := reFieldPart.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			field := strings.TrimFunc(temp[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == '=' })
			s.fields = append(s.fields, field)
			temp = strings.TrimSpace(temp[loc[1]:])
		} else {
			return errors.New("(field) cannot parse query, invalid token at: " + temp)
		}

		// secondly, parse the value part
		value, leftOver, err := _parseValue(temp, ',')
		if err == nil {
			s.values = append(s.values, value)
			temp = leftOver
			switch value.(type) {
			case placeholder:
				s.numInput++
			}
			continue
		}
		return err
	}
	return nil
}

func (s *StmtUpdate) parse(withOptsStr string) error {
	if err := s.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	if err := s._parseId(); err != nil {
		return err
	}

	if err := s._parseUpdateClause(); err != nil {
		return err
	}

	return nil
}

func (s *StmtUpdate) validate() error {
	if s.idStr == "" {
		return errors.New("id value is missing")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	if len(s.fields) == 0 {
		return errors.New("invalid query: SET clause is empty")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
//
// Note: this function expects the _partition key values are placed at the end_ of the argument list.
func (s *StmtUpdate) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.fetchPkInfo(); err != nil {
		return nil, err
	}
	if len(args) != s.numInput+s.numPkPaths {
		return nil, fmt.Errorf("expected %d arguments, got %d", s.numInput+s.numPkPaths, len(args))
	}

	// firstly, fetch the document
	id := s.idStr
	if s.id != nil {
		ph := s.id.(placeholder)
		if ph.index <= 0 || ph.index >= len(args) {
			return nil, fmt.Errorf("invalid value index %d", ph.index)
		}
		id = fmt.Sprintf("%s", args[ph.index-1])
	}
	docReq := DocReq{DbName: s.dbName, CollName: s.collName, DocId: id, PartitionKeyValues: s.extractPkValuesFromArgs(args...)}
	getDocResult := s.conn.restClient.GetDocument(docReq)
	if err := getDocResult.Error(); err != nil {
		result := buildResultNoResultSet(&getDocResult.RestReponse, false, "", 0)
		if getDocResult.StatusCode == 404 {
			// consider "document not found" as successful operation
			// but database/collection not found is not!
			if strings.Index(fmt.Sprintf("%s", err), "ResourceType: Document") >= 0 {
				result.err = nil
			}
		}
		return result, result.err
	}

	// secondly, update the fetched document
	etag := getDocResult.DocInfo.Etag()
	spec := DocumentSpec{DbName: s.dbName, CollName: s.collName, PartitionKeyValues: s.extractPkValuesFromArgs(args...), DocumentData: getDocResult.DocInfo.RemoveSystemAttrs()}
	for i := 0; i < len(s.fields); i++ {
		switch s.values[i].(type) {
		case placeholder:
			ph := s.values[i].(placeholder)
			if ph.index <= 0 || ph.index >= len(args) {
				return nil, fmt.Errorf("invalid value index %d", ph.index)
			}
			spec.DocumentData[s.fields[i]] = args[ph.index-1]
		default:
			spec.DocumentData[s.fields[i]] = s.values[i]
		}
	}
	replaceDocResult := s.conn.restClient.ReplaceDocument(etag, spec)
	result := buildResultNoResultSet(&replaceDocResult.RestReponse, false, "", 412)
	switch replaceDocResult.StatusCode {
	case 404: // rare case, but possible!
		// consider "document not found" as successful operation
		// but database/collection not found is not!
		if strings.Index(fmt.Sprintf("%s", replaceDocResult.Error()), "ResourceType: Document") >= 0 {
			result.err = nil
		}
	}
	return result, result.err
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtUpdate) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryNotSupported
}
