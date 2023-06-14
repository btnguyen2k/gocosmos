package gocosmos

import (
	"reflect"
	"testing"
)

func TestStmtInsert_parse(t *testing.T) {
	testName := "TestStmtInsert_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_no_collection", sql: `INSERT INTO db (a,b,c) VALUES (1,2,3)`, mustError: true},
		{name: "error_values", sql: `INSERT INTO db.table (a,b,c)`, mustError: true},
		{name: "error_columns", sql: `INSERT INTO db.table VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_string", sql: `INSERT INTO db.table (a) VALUES ('a string')`, mustError: true},
		{name: "error_invalid_string2", sql: `INSERT INTO db.table (a) VALUES ("a string")`, mustError: true},
		{name: "error_invalid_string3", sql: `INSERT INTO db.table (a) VALUES ("{key:value}")`, mustError: true},
		{name: "error_num_values_not_matched", sql: `INSERT INTO db.table (a,b) VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_number", sql: `INSERT INTO db.table (a,b) VALUES (0x1qa,2)`, mustError: true},
		{name: "error_invalid_string", sql: `INSERT INTO db.table (a,b) VALUES ("cannot \\"unquote",2)`, mustError: true},

		{
			name: "basic",
			sql: `INSERT INTO
db1.table1 (a, b, c, d, e, 
f) VALUES
	(null, 1.0, 
true, "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db1", collName: "table1"}, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders",
			sql: `INSERT 
INTO db-2.table_2 (
a,b,c) VALUES (
$1, :3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (1,2,3) WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, 3.0}},
		},
		{
			name:     "single_pk",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:     "singlepk_single_pk",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (1,2,@1) WITH singlePK, with SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, placeholder{1}}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v/%#v\nreceived %#v/%#v", testName+"/"+testCase.name, testCase.expected.StmtCRUD, testCase.expected, stmt.StmtCRUD, stmt)
			}
		})
	}
}

func TestStmtInsert_parse_defaultDb(t *testing.T) {
	testName := "TestStmtInsert_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_invalid_query", sql: `INSERT INTO .table (a,b) VALUES (1,2)`, mustError: true},
		{name: "error_invalid_query2", sql: `INSERT INTO db. (a,b) VALUES (1,2)`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `INSERT INTO
table1 (a, b, c, d, e,
f) VALUES
	(null, 1.0,
true, "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders_table_in_query",
			db:   "mydb",
			sql: `INSERT
INTO db-2.table_2 (
a,b,c) VALUES (
$1, :3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `INSERT INTO table (a,b,c) VALUES (1,2,3) WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, 3.0}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:     "singlepk_single_pk",
			db:       "mydb",
			sql:      `INSERT INTO table (a,b,c) VALUES (1,2,@1) WITH singlePK, with SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, placeholder{1}}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpsert_parse(t *testing.T) {
	testName := "TestStmtUpsert_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_no_collection", sql: `UPSERT INTO db (a,b,c) VALUES (1,2,3)`, mustError: true},
		{name: "error_values", sql: `UPSERT INTO db.table (a,b,c)`, mustError: true},
		{name: "error_columns", sql: `UPSERT INTO db.table VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_string", sql: `UPSERT INTO db.table (a) VALUES ('a string')`, mustError: true},
		{name: "error_invalid_string2", sql: `UPSERT INTO db.table (a) VALUES ("a string")`, mustError: true},
		{name: "error_invalid_string3", sql: `UPSERT INTO db.table (a) VALUES ("{key:value}")`, mustError: true},
		{name: "error_num_values_not_matched", sql: `UPSERT INTO db.table (a,b) VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_number", sql: `UPSERT INTO db.table (a,b) VALUES (0x1qa,2)`, mustError: true},
		{name: "error_invalid_string", sql: `UPSERT INTO db.table (a,b) VALUES ("cannot \\"unquote",2)`, mustError: true},

		{
			name: "basic",
			sql: `UPSERT INTO
db1.table1 (a,
b, c, d, e,
f) VALUES
	(null, 1.0, true,
  "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db1", collName: "table1"}, isUpsert: true, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders",
			sql: `UPSERT
INTO db-2.table_2 (
a,b,c) VALUES ($1,
	:3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "single_pk",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk_single_pk",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH SINGLE_PK, with singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpsert_parse_defaultDb(t *testing.T) {
	testName := "TestStmtUpsert_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_invalid_query", sql: `UPSERT INTO .table (a,b) VALUES (1,2)`, mustError: true},
		{name: "error_invalid_query2", sql: `UPSERT INTO db. (a,b) VALUES (1,2)`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `UPSERT INTO
table1 (a,
b, c, d, e,
f) VALUES
	(null, 1.0, true,
  "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, isUpsert: true, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders_table_in_query",
			db:   "mydb",
			sql: `UPSERT
INTO db-2.table_2 (
a,b,c) VALUES ($1,
	:3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES ($1, :3, @2) WITH SINGLEPK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `UPSERT INTO table (a,b,c) VALUES ($1, :3, @2) WITH single_pk`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk_single_pk",
			db:       "mydb",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES ($1, :3, @2) WITH single_pk WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v/%#v\nreceived %#v/%#v", testName+"/"+testCase.name, testCase.expected.StmtCRUD, testCase.expected, stmt.StmtCRUD, stmt)
			}
		})
	}
}

func TestStmtDelete_parse(t *testing.T) {
	testName := "TestStmtDelete_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtDelete
		mustError bool
	}{
		{name: "error_no_collection", sql: `DELETE FROM db WHERE id=1`, mustError: true},
		{name: "error_where", sql: `DELETE FROM db.table`, mustError: true},
		{name: "error_empty_id", sql: `DELETE FROM db.table WHERE id=`, mustError: true},
		{name: "error_invalid_value", sql: `DELETE FROM db.table WHERE id="1`, mustError: true},
		{name: "error_invalid_value2", sql: `DELETE FROM db.table WHERE id=2"`, mustError: true},
		{name: "error_invalid_where", sql: `DELETE FROM db.table WHERE id=@1 a`, mustError: true},
		{name: "error_invalid_where2", sql: `DELETE FROM db.table WHERE id=b $2`, mustError: true},
		{name: "error_invalid_where3", sql: `DELETE FROM db.table WHERE id=c :3 d`, mustError: true},

		{
			name: "basic",
			sql: `DELETE FROM 
db1.table1 WHERE 
	id=abc`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db1", collName: "table1"}, idStr: "abc"},
		},
		{
			name: "basic2",
			sql: `
	DELETE 
FROM db-2.table_2
	WHERE     id="def"`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, idStr: "def"},
		},
		{
			name: "basic3",
			sql: `DELETE FROM 
db_3-0.table-3_0 WHERE 
	id=@2`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db_3-0", collName: "table-3_0"}, idStr: "@2", id: placeholder{2}},
		},
		{
			name:     "singlepk",
			sql:      `DELETE FROM db.table WHERE id=@2 WITH singlePK`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, idStr: "@2", id: placeholder{2}},
		},
		{
			name:     "single_pk",
			sql:      `DELETE FROM db.table WHERE id=@2 with Single_PK`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, idStr: "@2", id: placeholder{2}},
		},
		{
			name:     "singlepk_single_pk",
			sql:      `DELETE FROM db.table WHERE id=@2 with SinglePK WITH SINGLE_PK`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, idStr: "@2", id: placeholder{2}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDelete)
			if !ok {
				t.Fatalf("%s failed: expected StmtDelete but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtDelete_parse_defaultDb(t *testing.T) {
	testName := "TestStmtDelete_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtDelete
		mustError bool
	}{
		{name: "error_invalid_query", sql: `DELETE FROM .table WHERE id=1`, mustError: true},
		{name: "error_invalid_query2", sql: `DELETE FROM db. WHERE id=1`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `DELETE FROM 
table1 WHERE 
	id=abc`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, idStr: "abc"},
		},
		{
			name: "db_in_query",
			db:   "mydb",
			sql: `
	DELETE 
FROM db-2.table_2
	WHERE     id="def"`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, idStr: "def"},
		},
		{
			name: "placeholder",
			db:   "mydb",
			sql: `DELETE FROM 
db_3-0.table-3_0 WHERE 
	id=@2`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db_3-0", collName: "table-3_0"}, idStr: "@2", id: placeholder{2}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `DELETE FROM table WHERE id=@2 With singlePk`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, idStr: "@2", id: placeholder{2}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `DELETE FROM db.table WHERE id=@2 With single_Pk`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, idStr: "@2", id: placeholder{2}},
		},
		{
			name:     "singlepk_single_pk",
			db:       "mydb",
			sql:      `DELETE FROM table WHERE id=@2 With single_Pk, With SinglePK`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, idStr: "@2", id: placeholder{2}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDelete)
			if !ok {
				t.Fatalf("%s failed: expected StmtDelete but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtSelect_parse(t *testing.T) {
	testName := "TestStmtSelect_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtSelect
		mustError bool
	}{
		{name: "error_db_and_collection", sql: `SELECT * FROM db.table`, mustError: true},
		{name: "error_no_collection", sql: `SELECT * WITH db=dbname`, mustError: true},
		{name: "error_no_db", sql: `SELECT * FROM c WITH collection=collname`, mustError: true},
		{name: "error_cross_partition_must_be_true", sql: `SELECT * FROM c WITH db=dbname WITH collection=collname WITH cross_partition=false`, mustError: true},

		{
			name:     "basic",
			sql:      `SELECT * FROM c WITH database=db WITH collection=tbl`,
			expected: &StmtSelect{dbName: "db", collName: "tbl", selectQuery: `SELECT * FROM c`, placeholders: map[int]string{}},
		},
		{
			name:     "cross_partition",
			sql:      `SELECT CROSS PARTITION * FROM c WHERE id="1" WITH db=db-1 WITH table=tbl_1`,
			expected: &StmtSelect{dbName: "db-1", collName: "tbl_1", isCrossPartition: true, selectQuery: `SELECT * FROM c WHERE id="1"`, placeholders: map[int]string{}},
		},
		{
			name:     "placeholders",
			sql:      `SELECT id,username,email FROM c WHERE username!=@1 AND (id>:2 OR email=$3) WITH CROSS_PARTITION=true WITH database=db_3-0 WITH table=table-3_0`,
			expected: &StmtSelect{dbName: "db_3-0", collName: "table-3_0", isCrossPartition: true, selectQuery: `SELECT id,username,email FROM c WHERE username!=@_1 AND (id>@_2 OR email=@_3)`, placeholders: map[int]string{1: "@_1", 2: "@_2", 3: "@_3"}},
		},
		{
			name:     "collection_in_query",
			sql:      `SELECT a,b,c FROM user u WHERE u.id="1" WITH db=dbtemp`,
			expected: &StmtSelect{dbName: "dbtemp", collName: "user", selectQuery: `SELECT a,b,c FROM user u WHERE u.id="1"`, placeholders: map[int]string{}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtSelect)
			if !ok {
				t.Fatalf("%s failed: expected StmtSelect but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtSelect_parse_defaultDb(t *testing.T) {
	testName := "TestStmtSelect_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtSelect
		mustError bool
	}{
		{
			name:     "basic",
			db:       "mydb",
			sql:      `SELECT * FROM c WITH collection=tbl`,
			expected: &StmtSelect{dbName: "mydb", collName: "tbl", selectQuery: `SELECT * FROM c`, placeholders: map[int]string{}},
		},
		{
			name:     "db_table_in_query",
			db:       "mydb",
			sql:      `SELECT CROSS PARTITION * FROM c WHERE id="1" WITH db=db-1 WITH table=tbl_1`,
			expected: &StmtSelect{dbName: "db-1", collName: "tbl_1", isCrossPartition: true, selectQuery: `SELECT * FROM c WHERE id="1"`, placeholders: map[int]string{}},
		},
		{
			name:     "placeholders",
			db:       "mydb",
			sql:      `SELECT id,username,email FROM c WHERE username!=@1 AND (id>:2 OR email=$3) WITH CROSS_PARTITION=true WITH table=tbl_2-0`,
			expected: &StmtSelect{dbName: "mydb", collName: "tbl_2-0", isCrossPartition: true, selectQuery: `SELECT id,username,email FROM c WHERE username!=@_1 AND (id>@_2 OR email=@_3)`, placeholders: map[int]string{1: "@_1", 2: "@_2", 3: "@_3"}},
		},
		{
			name:     "collection_in_query",
			db:       "mydb",
			sql:      `SELECT a,b,c FROM user u WHERE u.id="1"`,
			expected: &StmtSelect{dbName: "mydb", collName: "user", selectQuery: `SELECT a,b,c FROM user u WHERE u.id="1"`, placeholders: map[int]string{}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtSelect)
			if !ok {
				t.Fatalf("%s failed: expected StmtSelect but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpdate_parse(t *testing.T) {
	testName := "TestStmtUpdate_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtUpdate
		mustError bool
	}{
		{name: "error_no_collection", sql: `UPDATE db SET a=1,b=2,c=3 WHERE id=4`, mustError: true},
		{name: "error_where", sql: `UPDATE db.table SET a=1,b=2,c=3 WHERE username=4`, mustError: true},
		{name: "error_no_where", sql: `UPDATE db.table SET a=1,b=2,c=3`, mustError: true},
		{name: "error_no_set", sql: `UPDATE db.table WHERE id=1`, mustError: true},
		{name: "error_empty_set", sql: `UPDATE db.table SET      WHERE id=1`, mustError: true},
		{name: "error_invalid_value", sql: `UPDATE db.table SET a="{key:value}" WHERE id=1`, mustError: true},
		{name: "error_invalid_query", sql: `UPDATE db.table SET =1 WHERE id=2`, mustError: true},
		{name: "error_invalid_query2", sql: `UPDATE db.table SET a=1 WHERE id=   `, mustError: true},
		{name: "error_invalid_query3", sql: `UPDATE db.table SET a=1,b=2,c=3 WHERE id="4`, mustError: true},

		{
			name: "basic",
			sql: `UPDATE db1.table1 
SET a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]" WHERE
	id="abc"`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db1", collName: "table1"}, updateStr: `a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]"`, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}, idStr: "abc"},
		},
		{
			name: "basic2",
			sql: `UPDATE db-1.table_1 
SET a=$1, b=
	$2, c=:3, d=0 WHERE
	id=@4`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db-1", collName: "table_1"}, updateStr: `a=$1, b=
	$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "single_pk",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk_single_pk",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SINGLE_PK, With SinglePk`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtUpdate)
			if !ok {
				t.Fatalf("%s failed: expected StmtUpdate but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpdate_parse_defaultDb(t *testing.T) {
	testName := "TestStmtUpdate_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtUpdate
		mustError bool
	}{
		{name: "error_invalid_query", sql: `UPDATE .table SET a=1,b=2,c=3 WHERE id=4`, mustError: true},
		{name: "error_invalid_query2", sql: `UPDATE db. SET a=1,b=2,c=3 WHERE id=4`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `UPDATE table1 
SET a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]" WHERE
	id="abc"`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, updateStr: `a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]"`, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}, idStr: "abc"}},
		{
			name: "db_in_query",
			db:   "mydb",
			sql: `UPDATE db-1.table_1 
SET a=$1, b=
	$2, c=:3, d=0 WHERE
	id=@4`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db-1", collName: "table_1"}, updateStr: `a=$1, b=
	$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `UPDATE table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk_single_pk",
			db:       "mydb",
			sql:      `UPDATE table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SINGLE_PK, With SinglePk`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtUpdate)
			if !ok {
				t.Fatalf("%s failed: expected StmtUpdate but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}
