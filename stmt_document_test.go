package gocosmos

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStmtInsert_Query(t *testing.T) {
	testName := "TestStmtInsert_Query"
	db := _openDb(t, testName)
	_, err := db.Query("INSERT INTO db.table (a,b,c) VALUES (1,2,3)", nil)
	if err != ErrQueryNotSupported {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtInsert_Exec(t *testing.T) {
	testName := "TestStmtInsert_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "insert_new",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id, username, email, grade, actived) VALUES ("\"1\"", "\"user\"", "\"user@domain1.com\"", 7, true)`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "insert_conflict_pk",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,actived) VALUES ("\"1\"", "\"user\"", "\"user@domain2.com\"", 8, false)`, dbname),
			args:         []interface{}{"user"},
			mustConflict: true,
		},
		{
			name:         "insert_conflict_uk",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,actived) VALUES ("\"2\"", "\"user\"", "\"user@domain1.com\"", 9, false)`, dbname),
			args:         []interface{}{"user"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`, dbname),
			args:         []interface{}{"y"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists",
			sql:          `INSERT INTO db_not_exists.table (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`,
			args:         []interface{}{"y"},
			mustNotFound: true,
		},
		{
			name: "insert_new_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"1", "user", "user@domain1.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user"},
			affectedRows: 1,
		},
		{
			name:         "insert_conflict_pk_placeholders",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"1", "user", "user@domain2.com", 2, false, nil, "user"},
			mustConflict: true,
		},
		{
			name:         "insert_conflict_uk_placeholders",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbl_not_found (id,username,email) VALUES (:1, :2, :3)`, dbname),
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists_placeholders",
			sql:          `INSERT INTO db_not_exists.table (id,username,email) VALUES (@1, @2, @3)`,
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_value_index",
			sql:       fmt.Sprintf(`INSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :10)`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user"},
			mustError: "invalid value index",
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
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
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

func TestStmtInsert_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtInsert_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "insert_new",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          `INSERT INTO tbltemp (id, username, email, grade, actived) VALUES ("\"1\"", "\"user\"", "\"user@domain1.com\"", 7, true)`,
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "insert_conflict_pk",
			sql:          `INSERT INTO tbltemp (id,username,email,grade,actived) VALUES ("\"1\"", "\"user\"", "\"user@domain2.com\"", 8, false)`,
			args:         []interface{}{"user"},
			mustConflict: true,
		},
		{
			name:         "insert_conflict_uk",
			sql:          `INSERT INTO tbltemp (id,username,email,grade,actived) VALUES ("\"2\"", "\"user\"", "\"user@domain1.com\"", 9, false)`,
			args:         []interface{}{"user"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists",
			sql:          `INSERT INTO tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`,
			args:         []interface{}{"y"},
			mustNotFound: true,
		},
		{
			name: "insert_new_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          `INSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"1", "user", "user@domain1.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user"},
			affectedRows: 1,
		},
		{
			name:         "insert_conflict_pk_placeholders",
			sql:          fmt.Sprintf(`INSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"1", "user", "user@domain2.com", 2, false, nil, "user"},
			mustConflict: true,
		},
		{
			name:         "insert_conflict_uk_placeholders",
			sql:          `INSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          `INSERT INTO tbl_not_found (id,username,email) VALUES (:1, :2, :3)`,
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_value_index",
			sql:       `INSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :10)`,
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user"},
			mustError: "invalid value index",
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
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
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

func TestStmtUpsert_Query(t *testing.T) {
	testName := "TestStmtUpsert_Query"
	db := _openDb(t, testName)
	_, err := db.Query("UPSERT INTO db.table (a,b,c) VALUES (1,2,3)", nil)
	if err != ErrQueryNotSupported {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtUpsert_Exec(t *testing.T) {
	testName := "TestStmtUpsert_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "upsert_new",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH UK=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived) VALUES ("\"1\"", "\"user1\"", "\"user1@domain.com\"", 7, true)`, dbname),
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived) VALUES ("\"2\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`, dbname),
			args:         []interface{}{"user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,username,email,grade,actived) VALUES ("\"1\"", "\"user1\"", "\"user3@domain1.com\"", 8, false)`, dbname),
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,username,email,grade,actived) VALUES ("\"3\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`, dbname),
			args:         []interface{}{"user2"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`, dbname),
			args:         []interface{}{"y"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists",
			sql:          `UPSERT INTO db_not_exists.table (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`,
			args:         []interface{}{"y"},
			mustNotFound: true,
		},
		{
			name: "upsert_new_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"1", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"1", "user1", "user2@domain.com", 2, false, nil, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user1", "user2@domain.com", 3, false, nil, "user1"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbl_not_found (id,username,email) VALUES (:1, :2, :3)`, dbname),
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists_placeholders",
			sql:          `UPSERT INTO db_not_exists.table (id,username,email) VALUES (@1, @2, @3)`,
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_value_index",
			sql:       fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :10)`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user"},
			mustError: "invalid value index",
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
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
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

func TestStmtUpsert_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtUpsert_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "upsert_new",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH UK=/email", dbname),
			},
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, actived) VALUES ("\"1\"", "\"user1\"", "\"user1@domain.com\"", 7, true)`,
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, actived) VALUES ("\"2\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`,
			args:         []interface{}{"user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          `UPSERT INTO tbltemp (id,username,email,grade,actived) VALUES ("\"1\"", "\"user1\"", "\"user3@domain1.com\"", 8, false)`,
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          `UPSERT INTO tbltemp (id,username,email,grade,actived) VALUES ("\"3\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`,
			args:         []interface{}{"user2"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists",
			sql:          `UPSERT INTO tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`,
			args:         []interface{}{"y"},
			mustNotFound: true,
		},
		{
			name: "upsert_new_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"1", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"2", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"1", "user1", "user2@domain.com", 2, false, nil, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"2", "user1", "user2@domain.com", 3, false, nil, "user1"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          `UPSERT INTO tbl_not_found (id,username,email) VALUES (:1, :2, :3)`,
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_value_index",
			sql:       `UPSERT INTO tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :10)`,
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user"},
			mustError: "invalid value index",
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
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
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

func TestStmtDelete_Query(t *testing.T) {
	testName := "TestStmtDelete_Query"
	db := _openDb(t, testName)
	_, err := db.Query("DELETE FROM db.table WHERE id=1", nil)
	if err != ErrQueryNotSupported {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtDelete_Exec(t *testing.T) {
	testName := "TestStmtDelete_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		initParams   [][]interface{}
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "delete_1",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil, {"1", "user", "user@domain1.com", "user"}, {"2", "user", "user@domain2.com", "user"},
				{"3", "user", "user@domain3.com", "user"}, {"4", "user", "user@domain4.com", "user"}, {"5", "user", "user@domain5.com", "user"}},
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="2"`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_3",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=:1`, dbname),
			args:         []interface{}{"3", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_4",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=@1`, dbname),
			args:         []interface{}{"4", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$1`, dbname),
			args:         []interface{}{"5", "user"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 0,
		},
		{
			name:         "table_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.table_not_exists WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists",
			sql:          `DELETE FROM db_not_exists.table WHERE id=1`,
			args:         []interface{}{"user"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_value_index",
			sql:       fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$9`, dbname),
			args:      []interface{}{"1", "user"},
			mustError: "invalid value index",
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for i, initSql := range testCase.initSqls {
				var params []interface{}
				if len(testCase.initParams) > i {
					params = testCase.initParams[i]
				}
				_, err := db.Exec(initSql, params...)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
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

func TestStmtDelete_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtDelete_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		initParams   [][]interface{}
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "delete_1",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3)`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil, {"1", "user", "user@domain1.com", "user"}, {"2", "user", "user@domain2.com", "user"},
				{"3", "user", "user@domain3.com", "user"}, {"4", "user", "user@domain4.com", "user"}, {"5", "user", "user@domain5.com", "user"}},
			sql:          `DELETE FROM tbltemp WHERE id=1`,
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          `DELETE FROM tbltemp WHERE id="2"`,
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_3",
			sql:          `DELETE FROM tbltemp WHERE id=:1`,
			args:         []interface{}{"3", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_4",
			sql:          `DELETE FROM tbltemp WHERE id=@1`,
			args:         []interface{}{"4", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          `DELETE FROM tbltemp WHERE id=$1`,
			args:         []interface{}{"5", "user"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          `DELETE FROM tbltemp WHERE id=1`,
			args:         []interface{}{"user"},
			affectedRows: 0,
		},
		{
			name:         "table_not_exists",
			sql:          `DELETE FROM table_not_exists WHERE id=1`,
			args:         []interface{}{"user"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_value_index",
			sql:       `DELETE FROM tbltemp WHERE id=$9`,
			args:      []interface{}{"1", "user"},
			mustError: "invalid value index",
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for i, initSql := range testCase.initSqls {
				var params []interface{}
				if len(testCase.initParams) > i {
					params = testCase.initParams[i]
				}
				_, err := db.Exec(initSql, params...)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && err != ErrConflict {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && err != ErrNotFound {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
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
