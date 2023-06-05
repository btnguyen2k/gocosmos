package gocosmos

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestStmtCreateCollection_Query(t *testing.T) {
	testName := "TestStmtCreateCollection_Query"
	db := _openDb(t, testName)
	_, err := db.Query("CREATE COLLECTION dbtemp.tbltemp WITH pk=/id")
	if err != ErrQueryNotSupported {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtCreateCollection_Exec(t *testing.T) {
	testName := "TestStmtCreateCollection_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "create_new",
			initSqls:     []string{"DROP DATABASE IF EXISTS db_not_exists", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname)},
			sql:          fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/id WITH ru=400", dbname),
			affectedRows: 1,
		},
		{
			name:         "create_conflict",
			sql:          fmt.Sprintf("CREATE TABLE %s.tbltemp WITH pk=/id WITH ru=400", dbname),
			mustConflict: true,
		},
		{
			name:         "create_if_not_exists",
			sql:          fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.tbltemp WITH largepk=/a/b/c WITH maxru=4000 WITH uk=/a;/b,/c/d", dbname),
			affectedRows: 0,
		},
		{
			name:         "create_if_not_exists2",
			sql:          fmt.Sprintf("CREATE COLLECTION IF NOT EXISTS %s.tbltemp1 WITH largepk=/a/b/c WITH maxru=4000 WITH uk=/a;/b,/c/d", dbname),
			affectedRows: 1,
		},
		{
			name:         "create_not_found",
			sql:          "CREATE COLLECTION db_not_exists.table WITH pk=/a",
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for _, initSql := range testCase.initSqls {
				_, err := db.Exec(initSql)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
			_, err = execResult.LastInsertId()
			if err == nil {
				t.Fatalf("%s failed: expected LastInsertId but received nil", testName+"/"+testCase.name)
			}
			lastInsertId := make(map[string]interface{})
			err = json.Unmarshal([]byte(err.Error()), &lastInsertId)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/LastInsertId", err)
			}
			if len(lastInsertId) != 1 {
				t.Fatalf("%s failed - LastInsertId: %#v", testName+"/"+testCase.name+"/LastInsertId", lastInsertId)
			}
		})
	}
}

func TestStmtCreateCollection_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtCreateCollection_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "create_new",
			initSqls:     []string{fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname)},
			sql:          "CREATE COLLECTION tbltemp WITH pk=/id WITH ru=400",
			affectedRows: 1,
		},
		{
			name:         "create_conflict",
			sql:          "CREATE TABLE tbltemp WITH pk=/id WITH ru=400",
			mustConflict: true,
		},
		{
			name:         "create_if_not_exists",
			sql:          "CREATE TABLE IF NOT EXISTS tbltemp WITH largepk=/a/b/c WITH maxru=4000 WITH uk=/a;/b,/c/d",
			affectedRows: 0,
		},
		{
			name:         "create_if_not_exists2",
			sql:          "CREATE COLLECTION IF NOT EXISTS tbltemp1 WITH largepk=/a/b/c WITH maxru=4000 WITH uk=/a;/b,/c/d",
			affectedRows: 1,
		},
		{
			name:         "create_not_found",
			initSqls:     []string{fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname)},
			sql:          "CREATE COLLECTION table WITH pk=/a",
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for _, initSql := range testCase.initSqls {
				_, err := db.Exec(initSql)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
			_, err = execResult.LastInsertId()
			if err == nil {
				t.Fatalf("%s failed: expected LastInsertId but received nil", testName+"/"+testCase.name)
			}
			lastInsertId := make(map[string]interface{})
			err = json.Unmarshal([]byte(err.Error()), &lastInsertId)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/LastInsertId", err)
			}
			if len(lastInsertId) != 1 {
				t.Fatalf("%s failed - LastInsertId: %#v", testName+"/"+testCase.name+"/LastInsertId", lastInsertId)
			}
		})
	}
}

func TestStmtAlterCollection_Query(t *testing.T) {
	testName := "TestStmtAlterCollection_Query"
	db := _openDb(t, testName)
	_, err := db.Query("ALTER COLLECTION dbtemp.tbltemp WITH ru=400")
	if err != ErrQueryNotSupported {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtAlterCollection_Exec(t *testing.T) {
	testName := "TestStmtAlterCollection_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "change_ru",
			initSqls:     []string{"DROP DATABASE IF EXISTS db_not_exists", fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname), fmt.Sprintf("CREATE DATABASE %s", dbname), fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/id", dbname)},
			sql:          fmt.Sprintf("ALTER COLLECTION %s.tbltemp WITH ru=500", dbname),
			affectedRows: 1,
		},
		{
			name:         "change_maxru",
			sql:          fmt.Sprintf("ALTER TABLE %s.tbltemp WITH maxru=6000", dbname),
			affectedRows: 1,
		},
		{
			name:         "collection_not_found",
			sql:          fmt.Sprintf("ALTER COLLECTION %s.tbl_not_found WITH ru=400", dbname),
			mustNotFound: true,
		},
		{
			name:         "db_not_found",
			sql:          "ALTER COLLECTION db_not_exists.table WITH ru=400",
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for _, initSql := range testCase.initSqls {
				_, err := db.Exec(initSql)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
			_, err = execResult.LastInsertId()
			if err == nil {
				t.Fatalf("%s failed: expected LastInsertId but received nil", testName+"/"+testCase.name)
			}
			lastInsertId := make(map[string]interface{})
			err = json.Unmarshal([]byte(err.Error()), &lastInsertId)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/LastInsertId", err)
			}
			if len(lastInsertId) != 1 {
				t.Fatalf("%s failed - LastInsertId: %#v", testName+"/"+testCase.name+"/LastInsertId", lastInsertId)
			}
		})
	}
}

func TestStmtAlterCollection_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtAlterCollection_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "change_ru",
			initSqls:     []string{"DROP DATABASE IF EXISTS db_not_exists", fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname), fmt.Sprintf("CREATE DATABASE %s", dbname), fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/id", dbname)},
			sql:          "ALTER COLLECTION tbltemp WITH ru=500",
			affectedRows: 1,
		},
		{
			name:         "change_maxru",
			sql:          "ALTER TABLE tbltemp WITH maxru=6000",
			affectedRows: 1,
		},
		{
			name:         "collection_not_found",
			sql:          "ALTER COLLECTION tbl_not_found WITH ru=400",
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for _, initSql := range testCase.initSqls {
				_, err := db.Exec(initSql)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
			_, err = execResult.LastInsertId()
			if err == nil {
				t.Fatalf("%s failed: expected LastInsertId but received nil", testName+"/"+testCase.name)
			}
			lastInsertId := make(map[string]interface{})
			err = json.Unmarshal([]byte(err.Error()), &lastInsertId)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/LastInsertId", err)
			}
			if len(lastInsertId) != 1 {
				t.Fatalf("%s failed - LastInsertId: %#v", testName+"/"+testCase.name+"/LastInsertId", lastInsertId)
			}
		})
	}
}

func TestStmtDropCollection_Query(t *testing.T) {
	testName := "TestStmtDropCollection_Query"
	db := _openDb(t, testName)
	_, err := db.Query("DROP COLLECTION dbtemp.tbltemp")
	if err != ErrQueryNotSupported {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtDropCollection_Exec(t *testing.T) {
	testName := "TestStmtDropCollection_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "basic",
			initSqls:     []string{"DROP DATABASE IF EXISTS db_not_exists", fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname), fmt.Sprintf("CREATE DATABASE %s", dbname), fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/id", dbname)},
			sql:          fmt.Sprintf("DROP COLLECTION %s.tbltemp", dbname),
			affectedRows: 1,
		},
		{
			name:         "not_found",
			sql:          fmt.Sprintf("DROP TABLE %s.tbltemp", dbname),
			mustNotFound: true,
		},
		{
			name:         "if_exists",
			sql:          fmt.Sprintf("DROP COLLECTION IF EXISTS %s.tbltemp", dbname),
			affectedRows: 0,
		},
		{
			name:         "db_not_found",
			sql:          "DROP TABLE db_not_exists.table",
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for _, initSql := range testCase.initSqls {
				_, err := db.Exec(initSql)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
		})
	}
}

func TestStmtDropCollection_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtDropCollection_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "basic",
			initSqls:     []string{"DROP DATABASE IF EXISTS db_not_exists", fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname), fmt.Sprintf("CREATE DATABASE %s", dbname), fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/id", dbname)},
			sql:          "DROP COLLECTION tbltemp",
			affectedRows: 1,
		},
		{
			name:         "not_found",
			sql:          "DROP TABLE tbltemp",
			mustNotFound: true,
		},
		{
			name:         "if_exists",
			sql:          "DROP COLLECTION IF EXISTS tbltemp",
			affectedRows: 0,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for _, initSql := range testCase.initSqls {
				_, err := db.Exec(initSql)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
		})
	}
}

func TestStmtListCollections_Exec(t *testing.T) {
	testName := "TestStmtListCollections_Exec"
	db := _openDb(t, testName)
	_, err := db.Exec("LIST COLLECTIONS FROM dbtemp")
	if err != ErrExecNotSupported {
		t.Fatalf("%s failed: expected ErrExecNotSupported, but received %#v", testName, err)
	}
}

func TestStmtListCollections_Query(t *testing.T) {
	testName := "TestStmtListCollections_Query"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	db.Exec("DROP DATABASE IF EXISTS db_not_found")
	collNames := []string{"tbltemp", "tbltemp2", "tbltemp1"}
	db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	for _, collName := range collNames {
		db.Exec(fmt.Sprintf("CREATE COLLECTION %s.%s WITH pk=/id", dbname, collName))
	}

	dbRows, err := db.Query(fmt.Sprintf("LIST COLLECTIONS FROM %s", dbname))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbRows)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch_rows", err)
	}
	ok0, ok1, ok2 := false, false, false
	for _, row := range rows {
		if row["id"] == "tbltemp" {
			ok0 = true
		}
		if row["id"] == "tbltemp1" {
			ok1 = true
		}
		if row["id"] == "tbltemp2" {
			ok2 = true
		}
	}
	if !ok0 {
		t.Fatalf("%s failed: collection %s not found", testName, "tbltemp")
	}
	if !ok1 {
		t.Fatalf("%s failed: collection %s not found", testName, "tbltemp1")
	}
	if !ok2 {
		t.Fatalf("%s failed: collection %s not found", testName, "tbltemp2")
	}

	_, err = db.Query("LIST COLLECTIONS FROM db_not_found")
	if err != ErrNotFound {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", testName, err)
	}
}

func TestStmtListCollections_Query_DefaultDb(t *testing.T) {
	testName := "TestStmtListCollections_Query_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	db.Exec("DROP DATABASE IF EXISTS db_not_found")
	collNames := []string{"tbltemp", "tbltemp2", "tbltemp1"}
	db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	for _, collName := range collNames {
		db.Exec(fmt.Sprintf("CREATE COLLECTION %s.%s WITH pk=/id", dbname, collName))
	}

	dbRows, err := db.Query("LIST TABLES")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbRows)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch_rows", err)
	}
	ok0, ok1, ok2 := false, false, false
	for _, row := range rows {
		if row["id"] == "tbltemp" {
			ok0 = true
		}
		if row["id"] == "tbltemp1" {
			ok1 = true
		}
		if row["id"] == "tbltemp2" {
			ok2 = true
		}
	}
	if !ok0 {
		t.Fatalf("%s failed: collection %s not found", testName, "tbltemp")
	}
	if !ok1 {
		t.Fatalf("%s failed: collection %s not found", testName, "tbltemp1")
	}
	if !ok2 {
		t.Fatalf("%s failed: collection %s not found", testName, "tbltemp2")
	}

	_, err = db.Query("LIST COLLECTIONS FROM db_not_found")
	if err != ErrNotFound {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", testName, err)
	}
}
