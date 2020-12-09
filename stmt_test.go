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
		"CREATE COLLECTION db1.table1 WITH pk=/id":                                             {dbName: "db1", collName: "table1", ifNotExists: false, ru: 0, maxru: 0, pk: "/id", isLargePk: false, uk: nil},
		"create table db-2.table_2 WITH pk=/email WITH ru=100":                                 {dbName: "db-2", collName: "table_2", ifNotExists: false, ru: 100, maxru: 0, pk: "/email", isLargePk: false, uk: nil},
		"CREATE collection IF NOT EXISTS db_3.table-3 with largePK=/id WITH maxru=100":         {dbName: "db_3", collName: "table-3", ifNotExists: true, ru: 0, maxru: 100, pk: "/id", isLargePk: true, uk: nil},
		"create TABLE if not exists db-0_1.table_0-1 WITH pk=/a/b/c with uk=/a:/b,/c/d;/e/f/g": {dbName: "db-0_1", collName: "table_0-1", ifNotExists: true, ru: 0, maxru: 0, pk: "/a/b/c", isLargePk: false, uk: [][]string{{"/a"}, {"/b", "/c/d"}, {"/e/f/g"}}},
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
		}
	}

	// invalidQueries := []string{
	// 	"CREATE DATABASE dbtemp WITH ru=400 WITH maxru=1000",
	// 	"CREATE DATABASE dbtemp WITH ru=-1 WITH maxru=1000",
	// 	"CREATE DATABASE dbtemp WITH ru=400 WITH maxru=-1",
	// 	"CREATE DATABASE dbtemp WITH ru=-1",
	// 	"CREATE DATABASE dbtemp WITH maxru=-1",
	// }
	// for _, query := range invalidQueries {
	// 	if _, err := parseQuery(nil, query); err == nil {
	// 		t.Fatalf("%s failed: query must not be parsed/validated successfuly", name+"/"+query)
	// 	}
	// }
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
