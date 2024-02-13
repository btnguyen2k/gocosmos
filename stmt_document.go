package gocosmos

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/btnguyen2k/consu/g18"
)

var (
	reValNull          = regexp.MustCompile(`(?i)(null)\s*?`)
	reValNumber        = regexp.MustCompile(`([\d\.xe+-]+)\s*?`)
	reValBoolean       = regexp.MustCompile(`(?i)(true|false)\s*?`)
	reValString        = regexp.MustCompile(`(?i)("(\\"|[^"])*?")\s*?`)
	reValPlaceholder   = regexp.MustCompile(`(?i)[$@:](\d+)\s*?`)
	reValStringNoQuote = regexp.MustCompile(`(?i)([a-z0-9_./;:\\-]+)`)

	reFieldEqual = regexp.MustCompile(field + `\s*=(.*?)`)
)

type placeholder struct {
	index int
}

func _buildLeftover(input string, reSep *regexp.Regexp) string {
	if sep := reSep.FindString(input); sep != "" {
		input = input[len(sep):]
	}
	return input
}

func _parseValue(input string, separator rune) (value interface{}, leftOver string, err error) {
	reSep := regexp.MustCompile(`^\s*\` + string(separator) + `\s*`)

	if loc := reValPlaceholder.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimSpace(input[loc[0]+1 : loc[1]])
		index, err := strconv.Atoi(token)
		return placeholder{index}, _buildLeftover(input[loc[1]:], reSep), err
	}

	if loc := reValNull.FindStringIndex(input); loc != nil && loc[0] == 0 {
		return nil, _buildLeftover(input[loc[1]:], reSep), nil
	}

	if loc := reValNumber.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimSpace(input[loc[0]:loc[1]])
		var data interface{}
		err := json.Unmarshal([]byte(token), &data)
		if err != nil {
			err = errors.New("(number) cannot parse query, invalid token at: " + token)
		}
		return data, _buildLeftover(input[loc[1]:], reSep), err
	}

	if loc := reValBoolean.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimSpace(input[loc[0]:loc[1]])
		var data bool
		err := json.Unmarshal([]byte(token), &data)
		if err != nil {
			err = errors.New("(bool) cannot parse query, invalid token at: " + token)
		}
		return data, _buildLeftover(input[loc[1]:], reSep), err
	}

	if loc := reValString.FindStringIndex(input); loc != nil && loc[0] == 0 {
		var data interface{}
		token, err := strconv.Unquote(strings.TrimSpace(input[loc[0]:loc[1]]))
		if err == nil {
			err = json.Unmarshal([]byte(token), &data)
			if err != nil {
				err = errors.New("(unmarshal) cannot parse query, invalid token at: " + token)
			}
		} else {
			err = errors.New("(unquote) cannot parse query, invalid token at: " + token)
		}
		return data, _buildLeftover(input[loc[1]:], reSep), err
	}
	if loc := reValStringNoQuote.FindStringIndex(input); loc != nil && loc[0] == 0 {
		data := strings.TrimSpace(input[loc[0]:loc[1]])
		return data, _buildLeftover(input[loc[1]:], reSep), err
	}

	return nil, input, errors.New("cannot parse query, invalid token at: " + input)
}

func _parseFieldValueAnd(input string) (field string, value interface{}, leftOver string, err error) {
	reSep := regexp.MustCompile(`(?i)^\s*and\s+`)

	//must star with <field>\s*=
	if loc := reFieldEqual.FindStringSubmatchIndex(input); loc != nil && loc[0] == 0 {
		field = input[loc[2]:loc[3]]
		input = strings.TrimSpace(input[loc[1]:])

		if loc := reValPlaceholder.FindStringIndex(input); loc != nil && loc[0] == 0 {
			token := strings.TrimSpace(input[loc[0]+1 : loc[1]])
			index, err := strconv.Atoi(token)
			return field, placeholder{index}, _buildLeftover(input[loc[1]:], reSep), err
		}

		if loc := reValNull.FindStringIndex(input); loc != nil && loc[0] == 0 {
			return field, nil, _buildLeftover(input[loc[1]:], reSep), nil
		}

		if loc := reValNumber.FindStringIndex(input); loc != nil && loc[0] == 0 {
			token := strings.TrimSpace(input[loc[0]:loc[1]])
			var data interface{}
			err := json.Unmarshal([]byte(token), &data)
			if err != nil {
				err = errors.New("(number) cannot parse query, invalid token at: " + token)
			}
			return field, data, _buildLeftover(input[loc[1]:], reSep), err
		}

		if loc := reValBoolean.FindStringIndex(input); loc != nil && loc[0] == 0 {
			token := strings.TrimSpace(input[loc[0]:loc[1]])
			var data bool
			err := json.Unmarshal([]byte(token), &data)
			if err != nil {
				err = errors.New("(bool) cannot parse query, invalid token at: " + token)
			}
			return field, data, _buildLeftover(input[loc[1]:], reSep), err
		}

		if loc := reValString.FindStringIndex(input); loc != nil && loc[0] == 0 {
			var data interface{}
			token, err := strconv.Unquote(strings.TrimSpace(input[loc[0]:loc[1]]))
			if err == nil {
				err = json.Unmarshal([]byte(token), &data)
				if err != nil {
					err = errors.New("(unmarshal) cannot parse query, invalid token at: " + token)
				}
			} else {
				err = errors.New("(unquote) cannot parse query, invalid token at: " + token)
			}
			return field, data, _buildLeftover(input[loc[1]:], reSep), err
		}
		if loc := reValStringNoQuote.FindStringIndex(input); loc != nil && loc[0] == 0 {
			data := strings.TrimSpace(input[loc[0]:loc[1]])
			return field, data, _buildLeftover(input[loc[1]:], reSep), err
		}
	}
	return "", nil, input, errors.New("cannot parse query, invalid token at: " + input)
}

// StmtCRUD is abstract implementation of "INSERT|UPSERT|UPDATE|DELETE|SELECT" operations.
//
// @Available since v0.3.0
type StmtCRUD struct {
	*Stmt
	dbName         string
	collName       string
	isSinglePathPk bool
	withPk         string
	pkPaths        []string
	numPkPaths     int // number of PK paths
}

// String implements interface fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtCRUD) String() string {
	return fmt.Sprintf(`StmtCRUD{Stmt: %s, db: %q, collection: %q, is_single_pk: %v, with_pk: %q, pk_paths: %v, num_pk_paths: %d}`,
		s.Stmt, s.dbName, s.collName, s.isSinglePathPk, s.withPk, s.pkPaths, s.numPkPaths)
}

func (s *StmtCRUD) fetchPkInfo() error {
	if s.numPkPaths > 0 || s.conn == nil || s.isSinglePathPk {
		return nil
	}

	getCollResult := s.conn.restClient.GetCollection(s.dbName, s.collName)
	if getCollResult.Error() == nil {
		s.pkPaths = getCollResult.CollInfo.PartitionKey.Paths()
		s.numPkPaths = len(s.pkPaths)
	}
	return normalizeError(getCollResult.StatusCode, 0, getCollResult.Error())
}

func (s *StmtCRUD) parseWithOpts(withOptsStr string) error {
	if err := s.Stmt.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	if err := s.onlyOneWithOption("single PK path is specified more than once, only one of SINGLE_PK or SINGLEPK should be specified", "SINGLE_PK", "SINGLEPK"); err != nil {
		return err
	}
	if err := s.onlyOneWithOption("PK and SINGLE_PK/SINGLEPK must not be used together", "PK", "SINGLEPK"); err != nil {
		return err
	}
	if err := s.onlyOneWithOption("PK and SINGLE_PK/SINGLEPK must not be used together", "PK", "SINGLE_PK"); err != nil {
		return err
	}

	for k, v := range s.withOpts {
		switch k {
		case "SINGLE_PK", "SINGLEPK":
			if v == "" {
				s.isSinglePathPk = true
			} else {
				val, err := strconv.ParseBool(v)
				if err != nil || !val {
					return fmt.Errorf("invalid value at WITH %s (only value 'true' is accepted)", k)
				}
				s.isSinglePathPk = true
			}
		case "PK":
			s.withPk = "/" + strings.TrimLeft(v, "/")
		}
	}

	if s.withPk != "" {
		s.pkPaths = strings.Split(s.withPk, ",")
		s.numPkPaths = len(s.pkPaths)
	} else if s.isSinglePathPk {
		s.numPkPaths = 1
	}
	s.numInputs = 0
	return nil
}

// StmtInsert implements "INSERT" operation.
//
// Syntax:
//
//	INSERT|UPSERT INTO <db-name>.<collection-name>
//	(<field-list>)
//	VALUES (<value-list>)
//	[WITH PK=/pk-path]
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
//	- Using WITH PK is highly recommended to save one round-trip to server to fetch the collection's partition key info.
//	- If collection's PK has more than one path (i.e. sub-partition is used), the partition paths are comma separated, prefixed with '/', and must be specified in the same order as in the collection (.e.g. WITH PK=/field1,/field2...).
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

// String implements interface fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtInsert) String() string {
	return fmt.Sprintf(`StmtInsert{StmtCRUD: %s, upsert: %v, field_str: %q, value_str: %q, fields: %v, values: %v}`,
		s.StmtCRUD, s.isUpsert, s.fieldsStr, s.valuesStr, s.fields, s.values)
}

func (s *StmtInsert) parse(withOptsStr string) error {
	if err := s.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	for k := range s.withOpts {
		if k != "SINGLE_PK" && k != "SINGLEPK" && k != "PK" {
			return fmt.Errorf("invalid query, parsing error at WITH %s", k)
		}
	}

	s.fields = regexp.MustCompile(`[,\s]+`).Split(s.fieldsStr, -1)
	s.values = make([]interface{}, 0)
	for temp := strings.TrimSpace(s.valuesStr); temp != ""; temp = strings.TrimSpace(temp) {
		value, leftOver, err := _parseValue(temp, ',')
		if err == nil {
			s.values = append(s.values, value)
			temp = leftOver
			switch v := value.(type) {
			case placeholder:
				s.numInputs = g18.Max(s.numInputs, v.index)
			}
			continue
		}
		return err
	}

	return nil
}

func (s *StmtInsert) validate() error {
	if len(s.fields) != len(s.values) {
		return fmt.Errorf("number of fields (%d) does not match number of values (%d)", len(s.fields), len(s.values))
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	if s.isSinglePathPk {
		_, _ = fmt.Fprintf(os.Stderr, "[WARN] WITH singlePK/SINGLE_PK is deprecated, please use WITH PK instead\n")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
func (s *StmtInsert) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), _valuesToNamedValues(args))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v1.1.0
func (s *StmtInsert) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	// TODO: pass ctx to REST API client

	if err := s.fetchPkInfo(); err != nil {
		return nil, err
	}

	pkValues := make([]driver.NamedValue, s.numPkPaths)
	if n := len(args); n == s.numInputs+s.numPkPaths {
		_, _ = fmt.Fprintf(os.Stderr, "[WARN] supplying PK value at the end of parameter list is deprecated, please use WITH PK\n")
		copy(pkValues, args[s.numInputs:])
		args = args[:s.numInputs]
	} else if n == s.numInputs {
		fieldValMap := make(map[string]interface{})
		for i, field := range s.fields {
			fieldValMap[field] = s.values[i]
		}
		for i, pkPath := range s.pkPaths {
			v, ok := fieldValMap[pkPath[1:]]
			if !ok {
				return nil, fmt.Errorf("missing value for PK %s", pkPath)
			}
			pkValues[i] = driver.NamedValue{Name: pkPath[1:], Value: v}
		}
	} else {
		return nil, fmt.Errorf("expected %d or %d input values, got %d", s.numInputs, s.numInputs+s.numPkPaths, n)
	}

	spec := DocumentSpec{
		DbName:             s.dbName,
		CollName:           s.collName,
		IsUpsert:           s.isUpsert,
		PartitionKeyValues: make([]any, len(pkValues)),
		DocumentData:       make(map[string]any),
	}
	for i, pkValue := range pkValues {
		switch v := pkValue.Value.(type) {
		case placeholder:
			spec.PartitionKeyValues[i] = args[v.index-1].Value
		default:
			spec.PartitionKeyValues[i] = v
		}
	}
	for i, field := range s.fields {
		switch v := s.values[i].(type) {
		case placeholder:
			spec.DocumentData[field] = args[v.index-1].Value
		default:
			spec.DocumentData[field] = s.values[i]
		}
	}
	restResult := s.conn.restClient.CreateDocument(spec)
	rid := ""
	if restResult.DocInfo != nil {
		rid, _ = restResult.DocInfo["_rid"].(string)
	}
	result := buildResultNoResultSet(&restResult.RestResponse, true, rid, 0)
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
//	DELETE FROM <db-name>.<collection-name>
//	WHERE id=<id-value>
//	[AND pk1-path=<pk1-value> [AND pk2-path=<pk2-value> ...]]
//
//	- DELETE removes only one document specified by 'id'.
//	- The clause WHERE id=<id-value> is mandatory, and 'id' is a keyword, _not_ a field name.
//	- <id-value> and <pk-value> must be a placeholder (e.g. :1, @2 or $3), or JSON value.
//	- Supplying pk-paths and pk-values is highly recommended to save one round-trip to server to fetch the collection's partition key info.
//	- If collection's PK has more than one path (i.e. sub-partition is used), the partition paths must be specified in the same order as in the collection (.e.g. AND field1=value1 AND field2=value2...).
//
// See StmtInsert for details on <id-value> and <pk-value>.
type StmtDelete struct {
	*StmtCRUD
	whereStr string
	id       interface{}
	pkValues []interface{}
}

// String implements interface fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtDelete) String() string {
	return fmt.Sprintf(`StmtDelete{StmtCRUD: %s, where_clause: %q, id: %v, pk_values: %v}`,
		s.StmtCRUD, s.whereStr, s.id, s.pkValues)
}

func (s *StmtDelete) parse(withOptsStr string) error {
	if err := s.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	for k := range s.withOpts {
		if k != "SINGLE_PK" && k != "SINGLEPK" {
			return fmt.Errorf("invalid query, parsing error at WITH %s", k)
		}
	}

	s.pkPaths = make([]string, 0)
	s.pkValues = make([]interface{}, 0)
	for temp := strings.TrimSpace(s.whereStr); temp != ""; temp = strings.TrimSpace(temp) {
		pkPath, pkValue, leftOver, err := _parseFieldValueAnd(temp)
		if err == nil {
			if strings.ToLower(pkPath) == "id" {
				s.id = pkValue
			} else {
				s.pkPaths = append(s.pkPaths, "/"+strings.TrimLeft(pkPath, "/"))
				s.pkValues = append(s.pkValues, pkValue)
			}
			temp = leftOver
			switch v := pkValue.(type) {
			case placeholder:
				s.numInputs = g18.Max(s.numInputs, v.index)
			}
			continue
		}
		return err
	}
	if !s.isSinglePathPk {
		s.numPkPaths = len(s.pkPaths)
	}

	return nil
}

func (s *StmtDelete) validate() error {
	if s.id == nil {
		return errors.New("id value is missing")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	if s.isSinglePathPk {
		_, _ = fmt.Fprintf(os.Stderr, "[WARN] WITH singlePK/SINGLE_PK is deprecated, please use WHERE pk=value\n")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
func (s *StmtDelete) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), _valuesToNamedValues(args))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v1.1.0
func (s *StmtDelete) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	// TODO: pass ctx to REST API client

	if err := s.fetchPkInfo(); err != nil {
		return nil, err
	}

	pkValues := make([]driver.NamedValue, s.numPkPaths)
	if n := len(args); n == s.numInputs+s.numPkPaths {
		_, _ = fmt.Fprintf(os.Stderr, "[WARN] supplying PK value at the end of parameter list is deprecated, please use WHERE pk=value\n")
		copy(pkValues, args[s.numInputs:])
		args = args[:s.numInputs]
	} else if n == s.numInputs {
		for i, pkValue := range s.pkValues {
			pkValues[i] = driver.NamedValue{Name: s.pkPaths[i][1:], Value: pkValue}
		}
	} else {
		return nil, fmt.Errorf("expected %d or %d input values, got %d", s.numInputs, s.numInputs+s.numPkPaths, n)
	}

	id := s.id
	switch v := s.id.(type) {
	case placeholder:
		id = args[v.index-1].Value
	}
	id, _ = reddo.ToString(id)

	docReq := DocReq{
		DbName:             s.dbName,
		CollName:           s.collName,
		DocId:              id.(string),
		PartitionKeyValues: make([]any, len(pkValues)),
	}

	for i, pkValue := range pkValues {
		switch v := pkValue.Value.(type) {
		case placeholder:
			docReq.PartitionKeyValues[i] = args[v.index-1].Value
		default:
			docReq.PartitionKeyValues[i] = v
		}
	}

	restResult := s.conn.restClient.DeleteDocument(docReq)
	result := buildResultNoResultSet(&restResult.RestResponse, false, "", 0)
	switch restResult.StatusCode {
	case 404:
		// consider "document not found" as successful operation
		// but database/collection not found is not!
		if strings.Contains(fmt.Sprintf("%s", restResult.Error()), "ResourceType: Document") {
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
//	[WITH cross_partition|CrossPartition[=true]]
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

// String implements interface fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtSelect) String() string {
	return fmt.Sprintf(`StmtSelect{Stmt: %s, cross_partition: %v, db: %q, collection: %q}`,
		s.Stmt, s.isCrossPartition, s.dbName, s.collName)
}

func (s *StmtSelect) parse(withOptsStr string) error {
	if err := s.parseWithOpts(withOptsStr); err != nil {
		return err
	}

	if err := s.onlyOneWithOption("database is specified more than once, only one of DATABASE or DB should be specified", "DATABASE", "DB"); err != nil {
		return err
	}
	if err := s.onlyOneWithOption("collection is specified more than once, only one of COLLECTION or TABLE should be specified", "COLLECTION", "TABLE"); err != nil {
		return err
	}

	for k, v := range s.withOpts {
		switch k {
		case "DATABASE", "DB":
			s.dbName = v
		case "COLLECTION", "TABLE":
			s.collName = v
		case "CROSS_PARTITION", "CROSSPARTITION":
			if s.isCrossPartition {
				return fmt.Errorf("cross-partition is specified more than once, only one of CROSS_PARTITION or CrossPartition should be specified")
			}
			if v == "" {
				s.isCrossPartition = true
			} else {
				val, err := strconv.ParseBool(v)
				if err != nil || !val {
					return fmt.Errorf("invalid value at WITH %s (only value 'true' is accepted)", k)
				}
				s.isCrossPartition = true
			}
		default:
			return fmt.Errorf("invalid query, parsing error at WITH %s", k)
		}
	}

	matches := reValPlaceholder.FindAllStringSubmatch(s.selectQuery, -1)
	s.numInputs = len(matches)
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
	return s.QueryContext(context.Background(), _valuesToNamedValues(args))
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
//
// @Available since v1.1.0
func (s *StmtSelect) QueryContext(_ context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// TODO: pass ctx to REST API client

	params := make([]interface{}, 0)
	for i, arg := range args {
		v, ok := s.placeholders[i+1]
		if !ok {
			return nil, fmt.Errorf("there is no placeholder #%d", i+1)
		}
		params = append(params, map[string]interface{}{"name": v, "value": arg.Value})
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
//	UPDATE <db-name>.<collection-name>
//	SET <field-name1>=<value1>[,<field-nameN>=<valueN>]*
//	WHERE id=<id-value>
//	[AND pk1-path=<pk1-value> [AND pk2-path=<pk2-value> ...]]
//
//	- UPDATE modifies only one document specified by 'id'.
//	- The clause WHERE id=<id-value> is mandatory, and 'id' is a keyword, _not_ a field name.
//	- <id-value> and <pk-value> must be a placeholder (e.g. :1, @2 or $3), or JSON value.
//	- Supplying pk-paths and pk-values is highly recommended to save one round-trip to server to fetch the collection's partition key info.
//	- If collection's PK has more than one path (i.e. sub-partition is used), the partition paths must be specified in the same order as in the collection (.e.g. AND field1=value1 AND field2=value2...).
//
// See StmtInsert for details on <id-value> and <pk-value>.
type StmtUpdate struct {
	*StmtCRUD
	updateStr string
	whereStr  string
	id        interface{}
	fields    []string
	values    []interface{}
	pkValues  []interface{}
}

// String implements interface fmt.Stringer/String.
//
// @Available since v1.1.0
func (s *StmtUpdate) String() string {
	return fmt.Sprintf(`StmtUpdate{StmtCRUD: %s, fields: %v, values: %v, where_clause: %q, id: %v, pk_values: %v}`,
		s.StmtCRUD, s.fields, s.values, s.whereStr, s.id, s.pkValues)
}

var (
	reFieldPart = regexp.MustCompile(`\s*([\w\-]+)\s*=`)
)

func (s *StmtUpdate) _parseUpdateClause() error {
	s.fields = make([]string, 0)
	s.values = make([]interface{}, 0)
	for temp := strings.TrimSpace(s.updateStr); temp != ""; temp = strings.TrimSpace(temp) {
		// firstly, extract the field name
		if loc := reFieldPart.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			field := strings.TrimSpace(temp[loc[0] : loc[1]-1])
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
			switch v := value.(type) {
			case placeholder:
				s.numInputs = g18.Max(s.numInputs, v.index)
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

	for k := range s.withOpts {
		if k != "SINGLE_PK" && k != "SINGLEPK" {
			return fmt.Errorf("invalid query, parsing error at WITH %s", k)
		}
	}

	if err := s._parseUpdateClause(); err != nil {
		return err
	}

	s.pkPaths = make([]string, 0)
	s.pkValues = make([]interface{}, 0)
	for temp := strings.TrimSpace(s.whereStr); temp != ""; temp = strings.TrimSpace(temp) {
		pkPath, pkValue, leftOver, err := _parseFieldValueAnd(temp)
		if err == nil {
			if strings.ToLower(pkPath) == "id" {
				s.id = pkValue
			} else {
				s.pkPaths = append(s.pkPaths, "/"+strings.TrimLeft(pkPath, "/"))
				s.pkValues = append(s.pkValues, pkValue)
			}
			temp = leftOver
			switch v := pkValue.(type) {
			case placeholder:
				s.numInputs = g18.Max(s.numInputs, v.index)
			}
			continue
		}
		return err
	}
	if !s.isSinglePathPk {
		s.numPkPaths = len(s.pkPaths)
	}

	return nil
}

func (s *StmtUpdate) validate() error {
	if s.id == nil {
		return errors.New("id value is missing")
	}
	if s.dbName == "" || s.collName == "" {
		return errors.New("database/collection is missing")
	}
	if len(s.fields) == 0 {
		return errors.New("invalid query: SET clause is empty")
	}
	if s.isSinglePathPk {
		_, _ = fmt.Fprintf(os.Stderr, "[WARN] WITH singlePK/SINGLE_PK is deprecated, please use WHERE pk=value\n")
	}
	return nil
}

// Exec implements driver.Stmt/Exec.
func (s *StmtUpdate) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), _valuesToNamedValues(args))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v1.1.0
func (s *StmtUpdate) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	// TODO: pass ctx to REST API client

	if err := s.fetchPkInfo(); err != nil {
		return nil, err
	}

	pkValues := make([]driver.NamedValue, s.numPkPaths)
	if n := len(args); n == s.numInputs+s.numPkPaths {
		_, _ = fmt.Fprintf(os.Stderr, "[WARN] suplying PK value at the end of parameter list is deprecated, please use WHERE pk=value\n")
		copy(pkValues, args[s.numInputs:])
		args = args[:s.numInputs]
	} else if n == s.numInputs {
		for i, pkValue := range s.pkValues {
			pkValues[i] = driver.NamedValue{Name: s.pkPaths[i][1:], Value: pkValue}
		}
	} else {
		return nil, fmt.Errorf("expected %d or %d input values, got %d", s.numInputs, s.numInputs+s.numPkPaths, n)
	}
	pkValuesForApiCall := make([]any, len(pkValues))
	for i, pkValue := range pkValues {
		switch v := pkValue.Value.(type) {
		case placeholder:
			pkValuesForApiCall[i] = args[v.index-1].Value
		default:
			pkValuesForApiCall[i] = v
		}
	}

	// firstly, fetch the document
	id := s.id
	switch v := s.id.(type) {
	case placeholder:
		id = args[v.index-1].Value
	}
	id, _ = reddo.ToString(id)
	docReq := DocReq{
		DbName:             s.dbName,
		CollName:           s.collName,
		DocId:              id.(string),
		PartitionKeyValues: pkValuesForApiCall,
	}
	getDocResult := s.conn.restClient.GetDocument(docReq)
	if err := getDocResult.Error(); err != nil {
		result := buildResultNoResultSet(&getDocResult.RestResponse, false, "", 0)
		switch getDocResult.StatusCode {
		case 404:
			// consider "document not found" as successful operation
			// but database/collection not found is not!
			if strings.Contains(fmt.Sprintf("%s", err), "ResourceType: Document") {
				result.err = nil
			}
		}
		return result, result.err
	}

	// secondly, update the fetched document
	etag := getDocResult.DocInfo.Etag()
	spec := DocumentSpec{
		DbName:             s.dbName,
		CollName:           s.collName,
		PartitionKeyValues: pkValuesForApiCall,
		DocumentData:       getDocResult.DocInfo.RemoveSystemAttrs(),
	}
	for i, field := range s.fields {
		switch v := s.values[i].(type) {
		case placeholder:
			spec.DocumentData[field] = args[v.index-1].Value
		default:
			spec.DocumentData[field] = s.values[i]
		}
	}
	replaceDocResult := s.conn.restClient.ReplaceDocument(etag, spec)
	result := buildResultNoResultSet(&replaceDocResult.RestResponse, false, "", 412)
	switch replaceDocResult.StatusCode {
	case 404: // rare case, but possible!
		// consider "document not found" as successful operation
		// but database/collection not found is not!
		if strings.Contains(fmt.Sprintf("%s", replaceDocResult.Error()), "ResourceType: Document") {
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
