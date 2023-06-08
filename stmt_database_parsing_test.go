package gocosmos

import (
	"reflect"
	"testing"
)

func TestStmtCreateDatabase_parse(t *testing.T) {
	testName := "TestStmtCreateDatabase_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtCreateDatabase
		mustError bool
	}{
		{name: "error_no_table", sql: "CREATE DATABASE ", mustError: true},
		{name: "error_if_not_exists_no_table", sql: "CREATE DATABASE IF NOT EXISTS ", mustError: true},
		{name: "error_syntax", sql: "CREATE DATABASE db0 IF NOT EXISTS", mustError: true},
		{name: "error_if_exists", sql: "CREATE DATABASE if exists db0", mustError: true},
		{name: "error_if_not_exist", sql: "CREATE DATABASE IF NOT EXIST db0", mustError: true},
		{name: "error_ru_and_maxru", sql: "CREATE DATABASE db0 with RU=400, WITH MAXru=4000", mustError: true},

		{name: "basic", sql: "CREATE DATABASE  db1", expected: &StmtCreateDatabase{dbName: "db1"}},
		{name: "with_ru", sql: "create\ndatabase\n db-2 \nWITH \n ru=100", expected: &StmtCreateDatabase{dbName: "db-2", ru: 100}},
		{name: "with_max", sql: "CREATE\r\nDATABASE \n \r db_3 \r \n with\n\rmaxru=100", expected: &StmtCreateDatabase{dbName: "db_3", maxru: 100}},
		{name: "if_not_exists", sql: "CREATE DATABASE\tIF\rNOT\nEXISTS db-4-0", expected: &StmtCreateDatabase{dbName: "db-4-0", ifNotExists: true}},
		{name: "if_not_exists_with_ru", sql: "create\ndatabase IF NOT EXISTS db-5_0 with\nru=100", expected: &StmtCreateDatabase{dbName: "db-5_0", ifNotExists: true, ru: 100}},
		{name: "if_not_exists_with_maxru", sql: "CREATE DATABASE if not exists db_6-0 WITH maxru=100", expected: &StmtCreateDatabase{dbName: "db_6-0", ifNotExists: true, maxru: 100}},
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
			stmt, ok := s.(*StmtCreateDatabase)
			if !ok {
				t.Fatalf("%s failed: expected StmtCreateDatabase but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.withOptsStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtAlterDatabase_parse(t *testing.T) {
	testName := "TestStmtAlterDatabase_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtAlterDatabase
		mustError bool
	}{
		{name: "error_no_ru_maxru", sql: "ALTER database db0", mustError: true},
		{name: "error_ru_and_maxru", sql: "ALTER database db0 WITH RU=400, WITH maxRU=4000", mustError: true},

		{name: "with_ru", sql: "ALTER\rdatabase\ndb1\tWITH ru=400", expected: &StmtAlterDatabase{dbName: "db1", ru: 400}},
		{name: "with_maxru", sql: "alter DATABASE db-1 with maxru=4000", expected: &StmtAlterDatabase{dbName: "db-1", maxru: 4000}},
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
			stmt, ok := s.(*StmtAlterDatabase)
			if !ok {
				t.Fatalf("%s failed: expected StmtAlterDatabase but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.withOptsStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtDropDatabase_parse(t *testing.T) {
	testName := "TestStmtDropDatabase_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtDropDatabase
		mustError bool
	}{
		{name: "error_if_exist", sql: "DROP DATABASE IF EXIST db1", mustError: true},
		{name: "error_no_db", sql: "DROP DATABASE ", mustError: true},

		{name: "basic", sql: "DROP DATABASE  db1", expected: &StmtDropDatabase{dbName: "db1"}},
		{name: "lfcr", sql: "DROP\ndatabase\rdb-2", expected: &StmtDropDatabase{dbName: "db-2"}},
		{name: "if_exists", sql: "drop\rdatabase\nIF\nEXISTS db_3", expected: &StmtDropDatabase{dbName: "db_3", ifExists: true}},
		{name: "if_exists_2", sql: "Drop Database \tIf\t Exists \t db-4_0", expected: &StmtDropDatabase{dbName: "db-4_0", ifExists: true}},
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
			stmt, ok := s.(*StmtDropDatabase)
			if !ok {
				t.Fatalf("%s failed: expected StmtDropDatabase but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtListDatabases_parse(t *testing.T) {
	testName := "TestStmtListDatabases_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtListDatabases
		mustError bool
	}{
		{name: "basic", sql: "LIST DATABASES", expected: &StmtListDatabases{}},
		{name: "database", sql: " lisT \r\t\n Database ", expected: &StmtListDatabases{}},
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
			stmt, ok := s.(*StmtListDatabases)
			if !ok {
				t.Fatalf("%s failed: expected StmtListDatabases but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}
