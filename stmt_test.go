package go_cosmos

import (
	"testing"
)

func TestStmt_NumInput(t *testing.T) {
	name := "TestStmt_NumInput"
	testData := map[string]int{
		"CREATE DATABASE dbtemp":               0,
		"DROP DATABASE dbtemp":                 0,
		"CREATE DATABASE IF NOT EXISTS dbtemp": 0,
		"DROP DATABASE IF EXISTS dbtemp":       0,

		"CREATE TABLE tbltemp":                    0,
		"DROP TABLE tbltemp":                      0,
		"CREATE TABLE IF NOT EXISTS tbltemp":      0,
		"DROP TABLE IF EXISTS tbltemp":            0,
		"CREATE COLLECTION tbltemp":               0,
		"DROP COLLECTION tbltemp":                 0,
		"CREATE COLLECTION IF NOT EXISTS tbltemp": 0,
		"DROP COLLECTION IF EXISTS tbltemp":       0,

		"SELECT * FROM tbltemp WHERE id=@1 AND email=$2 OR username=:3": 3,
		"INSERT INTO tbltemp (id, name) VALUES ($1, :2)":                2,
		"DELETE FROM tbltemp WHERE id=@1":                               1,
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
		ru          int
	}
	testData := map[string]testStruct{
		"CREATE DATABASE db1":                           {dbName: "db1", ifNotExists: false, ru: 0},
		"CREATE DATABASE db2 WITH ru=0":                 {dbName: "db2", ifNotExists: false, ru: 0},
		"CREATE DATABASE db3 WITH ru=100":               {dbName: "db3", ifNotExists: false, ru: 100},
		"CREATE DATABASE IF NOT EXISTS db4":             {dbName: "db4", ifNotExists: true, ru: 0},
		"CREATE DATABASE IF NOT EXISTS db5 WITH ru=0":   {dbName: "db5", ifNotExists: true, ru: 0},
		"CREATE DATABASE IF NOT EXISTS db6 WITH ru=100": {dbName: "db6", ifNotExists: true, ru: 100},
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
		"DROP DATABASE db1":           {dbName: "db1", ifExists: false},
		"DROP DATABASE IF EXISTS db4": {dbName: "db4", ifExists: true},
	}

	for query, data := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if dbstmt, ok := stmt.(*StmtDropDatabase); !ok {
			t.Fatalf("%s failed: the parsed stmt must be of type *StmtCreateDatabase", name+"/"+query)
		} else if dbstmt.dbName != data.dbName {
			t.Fatalf("%s failed: <db-name> expected %#v but received %#v", name+"/"+query, data.dbName, dbstmt.dbName)
		} else if dbstmt.ifExists != data.ifExists {
			t.Fatalf("%s failed: <if-exists> expected %#v but received %#v", name+"/"+query, data.ifExists, dbstmt.ifExists)
		}
	}
}
