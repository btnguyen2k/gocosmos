package go_cosmos

import (
	"reflect"
	"testing"
)

func TestStmt_NumInput(t *testing.T) {
	name := "TestStmt_NumInput"
	testData := map[string]int{
		"CREATE DATABASE dbtemp":               0,
		"DROP DATABASE dbtemp":                 0,
		"CREATE DATABASE IF NOT EXISTS dbtemp": 0,
		"DROP DATABASE IF EXISTS dbtemp":       0,

		"CREATE TABLE db.tbltemp WITH pk=/id":                    0,
		"DROP TABLE db.tbltemp":                                  0,
		"CREATE TABLE IF NOT EXISTS db.tbltemp WITH pk=/id":      0,
		"DROP TABLE IF EXISTS db.tbltemp":                        0,
		"CREATE COLLECTION db.tbltemp WITH pk=/id":               0,
		"DROP COLLECTION db.tbltemp":                             0,
		"CREATE COLLECTION IF NOT EXISTS db.tbltemp WITH pk=/id": 0,
		"DROP COLLECTION IF EXISTS db.tbltemp":                   0,

		// "SELECT * FROM tbltemp WHERE id=@1 AND email=$2 OR username=:3": 3,
		"INSERT INTO db.tbltemp (id, name, email) VALUES ($1, :2, @3)": 4,
		// "DELETE FROM tbltemp WHERE id=$1 OR (email=:2 AND username=@3)":                               3,
	}

	for query, numInput := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if v := stmt.NumInput(); v != numInput {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/"+query, numInput, v)
		}
	}
}

func Test_parseQuery_CreateDatabase(t *testing.T) {
	name := "Test_parseQuery_CreateDatabase"
	type testStruct struct {
		dbName      string
		ifNotExists bool
		ru, maxru   int
	}
	testData := map[string]testStruct{
		"CREATE DATABASE db1":                                 {dbName: "db1", ifNotExists: false, ru: 0, maxru: 0},
		"create database db-2 WITH ru=100":                    {dbName: "db-2", ifNotExists: false, ru: 100, maxru: 0},
		"CREATE DATABASE db_3 with maxru=100":                 {dbName: "db_3", ifNotExists: false, ru: 0, maxru: 100},
		"CREATE DATABASE IF NOT EXISTS db-4-0":                {dbName: "db-4-0", ifNotExists: true, ru: 0, maxru: 0},
		"create database IF NOT EXISTS db-5_0 with ru=100":    {dbName: "db-5_0", ifNotExists: true, ru: 100, maxru: 0},
		"CREATE DATABASE if not exists db_6-0 WITH maxru=100": {dbName: "db_6-0", ifNotExists: true, ru: 0, maxru: 100},
	}
	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtCreateDatabase); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtCreateDatabase", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.ifNotExists != data.ifNotExists {
			t.Fatalf("%s failed: <if-not-exists> expected %#v but received %#v", name+"/"+query, data.ifNotExists, dbstmt.ifNotExists)
		} else if dbstmt.ru != data.ru {
			t.Fatalf("%s failed: <ru> expected %#v but received %#v", name+"/"+query, data.ru, dbstmt.ru)
		} else if dbstmt.maxru != data.maxru {
			t.Fatalf("%s failed: <maxru> expected %#v but received %#v", name+"/"+query, data.maxru, dbstmt.maxru)
		}
	}

	invalidQueries := []string{
		"CREATE DATABASE dbtemp WITH ru=400 WITH maxru=1000",
		"CREATE DATABASE dbtemp WITH ru=-1 WITH maxru=1000",
		"CREATE DATABASE dbtemp WITH ru=400 WITH maxru=-1",
		"CREATE DATABASE dbtemp WITH ru=-1",
		"CREATE DATABASE dbtemp WITH maxru=-1",
	}
	for _, query := range invalidQueries {
		if _, err := parseQuery(nil, query); err == nil {
			t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
		}
	}
}

func Test_parseQuery_DropDatabase(t *testing.T) {
	name := "Test_parseQuery_DropDatabase"
	type testStruct struct {
		dbName   string
		ifExists bool
	}
	testData := map[string]testStruct{
		"DROP DATABASE db1":              {dbName: "db1", ifExists: false},
		"DROP database db-2":             {dbName: "db-2", ifExists: false},
		"drop database IF EXISTS db_3":   {dbName: "db_3", ifExists: true},
		"Drop Database If Exists db-4_0": {dbName: "db-4_0", ifExists: true},
	}

	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtDropDatabase); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtDropDatabase", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.ifExists != data.ifExists {
			t.Fatalf("%s failed: <if-exists> expected %#v but received %#v", name+"/"+query, data.ifExists, dbstmt.ifExists)
		}
	}
}

func Test_parseQuery_ListDatabases(t *testing.T) {
	name := "Test_parseQuery_ListDatabases"
	testData := []string{"LIST DATABASES", "list database"}

	for _, query := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if _, ok := stmt.(*StmtListDatabases); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtListDatabases", name+"/"+query)
		}
	}
}

/*----------------------------------------------------------------------*/

func Test_parseQuery_CreateCollection(t *testing.T) {
	name := "Test_parseQuery_CreateCollection"
	type testStruct struct {
		dbName      string
		collName    string
		ifNotExists bool
		ru, maxru   int
		pk          string
		isLargePk   bool
		uk          [][]string
	}
	testData := map[string]testStruct{
		"CREATE COLLECTION db1.table1 WITH pk=/id":                                                  {dbName: "db1", collName: "table1", ifNotExists: false, ru: 0, maxru: 0, pk: "/id", isLargePk: false, uk: nil},
		"create table db-2.table_2 WITH PK=/email WITH ru=100":                                      {dbName: "db-2", collName: "table_2", ifNotExists: false, ru: 100, maxru: 0, pk: "/email", isLargePk: false, uk: nil},
		"CREATE collection IF NOT EXISTS db_3.table-3 with largePK=/id WITH maxru=100":              {dbName: "db_3", collName: "table-3", ifNotExists: true, ru: 0, maxru: 100, pk: "/id", isLargePk: true, uk: nil},
		"create TABLE if not exists db-0_1.table_0-1 WITH LARGEpk=/a/b/c with uk=/a:/b,/c/d;/e/f/g": {dbName: "db-0_1", collName: "table_0-1", ifNotExists: true, ru: 0, maxru: 0, pk: "/a/b/c", isLargePk: false, uk: [][]string{{"/a"}, {"/b", "/c/d"}, {"/e/f/g"}}},
	}
	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtCreateCollection); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtCreateCollection", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.collName != data.collName {
			t.Fatalf("%s failed: <collection-name> expected %#v but received %#v", name+"/"+query, data.collName, dbstmt.collName)
		} else if dbstmt.ifNotExists != data.ifNotExists {
			t.Fatalf("%s failed: <if-not-exists> expected %#v but received %#v", name+"/"+query, data.ifNotExists, dbstmt.ifNotExists)
		} else if dbstmt.ru != data.ru {
			t.Fatalf("%s failed: <ru> expected %#v but received %#v", name+"/"+query, data.ru, dbstmt.ru)
		} else if dbstmt.maxru != data.maxru {
			t.Fatalf("%s failed: <maxru> expected %#v but received %#v", name+"/"+query, data.maxru, dbstmt.maxru)
		} else if dbstmt.pk != data.pk {
			t.Fatalf("%s failed: <pk> expected %#v but received %#v", name+"/"+query, data.pk, dbstmt.pk)
		} else if !reflect.DeepEqual(dbstmt.uk, data.uk) {
			t.Fatalf("%s failed: <uk> expected %#v but received %#v", name+"/"+query, data.uk, dbstmt.uk)
		}
	}

	invalidQueries := []string{
		"CREATE collection db.coll",
		"CREATE collection db.coll WITH pk=/a WITH largepk=/b",
		"CREATE collection db.coll WITH pk=",
		"CREATE collection db.coll WITH largepk=",
		"CREATE collection db.coll WITH pk=/id WITH ru=400 WITH maxru=1000",
		"create TABLE db.coll WITH pk=/id WITH ru=-1 WITH maxru=1000",
		"CREATE COLLECTION db.coll WITH pk=/id WITH ru=400 WITH maxru=-1",
		"CREATE TABLE db.table WITH pk=/id WITH ru=-1",
		"CREATE COLLECTION db.table WITH pk=/id WITH ru=-1",
		"CREATE TABLE db WITH pk=/id", // no collection name
	}
	for _, query := range invalidQueries {
		if _, err := parseQuery(nil, query); err == nil {
			t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
		}
	}
}

func Test_parseQuery_DropCollection(t *testing.T) {
	name := "Test_parseQuery_DropCollection"
	type testStruct struct {
		dbName   string
		collName string
		ifExists bool
	}
	testData := map[string]testStruct{
		"DROP COLLECTION db1.table1":             {dbName: "db1", collName: "table1", ifExists: false},
		"DROP table db-2.table_2":                {dbName: "db-2", collName: "table_2", ifExists: false},
		"drop collection IF EXISTS db_3.table-3": {dbName: "db_3", collName: "table-3", ifExists: true},
		"Drop Table If Exists db-4_0.table_4-0":  {dbName: "db-4_0", collName: "table_4-0", ifExists: true},
	}

	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtDropCollection); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtDropDatabase", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.collName != data.collName {
			t.Fatalf("%s failed: <collection-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.ifExists != data.ifExists {
			t.Fatalf("%s failed: <if-exists> expected %#v but received %#v", name+"/"+query, data.ifExists, dbstmt.ifExists)
		}
	}

	invalidQueries := []string{
		"DROP collection db", // no collection name
		"drop TABLE db",      // no collection name
	}
	for _, query := range invalidQueries {
		if _, err := parseQuery(nil, query); err == nil {
			t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
		}
	}
}

func Test_parseQuery_ListCollections(t *testing.T) {
	name := "Test_parseQuery_ListCollections"
	testData := map[string]string{
		"LIST COLLECTIONS from db1": "db1",
		"list collection FROM db-2": "db-2",
		"LIST tables FROM db_3":     "db_3",
		"list TABLE from db-4_0":    "db-4_0",
	}

	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtListCollections); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtListDatabases", name+"/"+query)
		} else if dbstmt.dbName != data {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data, dbstmt.dbName)
		}
	}
}

func Test_parseQuery_Insert(t *testing.T) {
	name := "Test_parseQuery_Insert"
	type testStruct struct {
		dbName   string
		collName string
		fields   []string
		values   []interface{}
	}
	testData := map[string]testStruct{
		`INSERT INTO db1.table1 (a, b, c, d, e, f) VALUES (null, 1.0, true, "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`: {
			dbName: "db1", collName: "table1", fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{
				nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`},
			},
		},
		`INSERT INTO db-2.table_2 (a,b,c) VALUES ($1, :3, @2)`: {
			dbName: "db-2", collName: "table_2", fields: []string{"a", "b", "c"}, values: []interface{}{
				placeholder{1}, placeholder{3}, placeholder{2},
			},
		},
	}
	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtInsert); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtInsert", name+"/"+query)
		} else if dbstmt.isUpsert {
			t.Fatalf("%s failed: is-upsert must be disabled", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.collName != data.collName {
			t.Fatalf("%s failed: <collection-name> expected %#v but received %#v", name+"/"+query, data.collName, dbstmt.collName)
		} else if !reflect.DeepEqual(dbstmt.fields, data.fields) {
			t.Fatalf("%s failed: <fields> expected %#v but received %#v", name+"/"+query, data.fields, dbstmt.fields)
		} else if !reflect.DeepEqual(dbstmt.values, data.values) {
			t.Fatalf("%s failed: <values> expected %#v but received %#v", name+"/"+query, data.values, dbstmt.values)
		}
	}

	invalidQueries := []string{
		`INSERT INTO db (a,b,c) VALUES (1,2,3)`,           // no collection name
		`INSERT INTO db.table (a,b,c)`,                    // no VALUES part
		`INSERT INTO db.table VALUES (1,2,3)`,             // no column list
		`INSERT INTO db.table (a) VALUES ('a string')`,    // invalid string literature
		`INSERT INTO db.table (a) VALUES ("a string")`,    // should be "\"a string\""
		`INSERT INTO db.table (a) VALUES ("{key:value}")`, // should be "{\"key\:\"value\"}"
		`INSERT INTO db.table (a,b) VALUES (1,2,3)`,       // number of field and value mismatch
	}
	for _, query := range invalidQueries {
		if _, err := parseQuery(nil, query); err == nil {
			t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
		}
	}
}

func Test_parseQuery_Upsert(t *testing.T) {
	name := "Test_parseQuery_Upsert"
	type testStruct struct {
		dbName   string
		collName string
		fields   []string
		values   []interface{}
	}
	testData := map[string]testStruct{
		`UPSERT INTO db1.table1 (a, b, c, d, e, f) VALUES (null, 1.0, true, "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`: {
			dbName: "db1", collName: "table1", fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{
				nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`},
			},
		},
		`UPSERT INTO db-2.table_2 (a,b,c) VALUES ($1, :3, @2)`: {
			dbName: "db-2", collName: "table_2", fields: []string{"a", "b", "c"}, values: []interface{}{
				placeholder{1}, placeholder{3}, placeholder{2},
			},
		},
	}
	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtInsert); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtInsert", name+"/"+query)
		} else if !dbstmt.isUpsert {
			t.Fatalf("%s failed: is-upsert must be enabled", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.collName != data.collName {
			t.Fatalf("%s failed: <collection-name> expected %#v but received %#v", name+"/"+query, data.collName, dbstmt.collName)
		} else if !reflect.DeepEqual(dbstmt.fields, data.fields) {
			t.Fatalf("%s failed: <fields> expected %#v but received %#v", name+"/"+query, data.fields, dbstmt.fields)
		} else if !reflect.DeepEqual(dbstmt.values, data.values) {
			t.Fatalf("%s failed: <values> expected %#v but received %#v", name+"/"+query, data.values, dbstmt.values)
		}
	}

	invalidQueries := []string{
		`UPSERT INTO db (a,b,c) VALUES (1,2,3)`,           // no collection name
		`UPSERT INTO db.table (a,b,c)`,                    // no VALUES part
		`UPSERT INTO db.table VALUES (1,2,3)`,             // no column list
		`UPSERT INTO db.table (a) VALUES ('a string')`,    // invalid string literature
		`UPSERT INTO db.table (a) VALUES ("a string")`,    // should be "\"a string\""
		`UPSERT INTO db.table (a) VALUES ("{key:value}")`, // should be "{\"key\:\"value\"}"
		`UPSERT INTO db.table (a,b) VALUES (1,2,3)`,       // number of field and value mismatch
	}
	for _, query := range invalidQueries {
		if _, err := parseQuery(nil, query); err == nil {
			t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
		}
	}
}

func Test_parseQuery_Delete(t *testing.T) {
	name := "Test_parseQuery_Delete"
	type testStruct struct {
		dbName   string
		collName string
		idStr    string
		id       interface{}
	}
	testData := map[string]testStruct{
		`DELETE FROM db1.table1 WHERE id=abc`:      {dbName: "db1", collName: "table1", idStr: "abc", id: nil},
		`DELETE FROM db-2.table_2 WHERE id="def"`:  {dbName: "db-2", collName: "table_2", idStr: "def", id: nil},
		`DELETE FROM db_3-0.table-3_0 WHERE id=@2`: {dbName: "db_3-0", collName: "table-3_0", idStr: "@2", id: placeholder{2}},
	}
	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtDelete); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtDelete", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.collName != data.collName {
			t.Fatalf("%s failed: <collection-name> expected %#v but received %#v", name+"/"+query, data.collName, dbstmt.collName)
		} else if dbstmt.idStr != data.idStr {
			t.Fatalf("%s failed: <id-str> expected %#v but received %#v", name+"/"+query, data.idStr, dbstmt.idStr)
		} else if !reflect.DeepEqual(dbstmt.id, data.id) {
			t.Fatalf("%s failed: <id> expected %#v but received %#v", name+"/"+query, data.id, dbstmt.id)
		}
	}

	invalidQueries := []string{
		`DELETE FROM db WHERE id=1`,      // no collection name
		`DELETE FROM db.table`,           // no WHERE part
		`DELETE FROM db.table WHERE id=`, // id is empty
		`DELETE FROM db.table WHERE id="1`,
		`DELETE FROM db.table WHERE id=2"`,
		`DELETE FROM db.table WHERE id=@1 a`,
		`DELETE FROM db.table WHERE id=b $2`,
		`DELETE FROM db.table WHERE id=c :3 d`,
	}
	for _, query := range invalidQueries {
		if _, err := parseQuery(nil, query); err == nil {
			t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
		}
	}
}
