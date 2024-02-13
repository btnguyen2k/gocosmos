package gocosmos_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btnguyen2k/gocosmos"
	"strings"
	"testing"
	"time"
)

func TestStmtUpsert_Query(t *testing.T) {
	testName := "TestStmtUpsert_Query"
	db := _openDb(t, testName)
	_, err := db.Query("UPSERT INTO db.table (a,b,c) VALUES (1,2,3)", nil)
	if !errors.Is(err, gocosmos.ErrQueryNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtUpsert_Exec(t *testing.T) {
	testName := "TestStmtUpsert_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active) VALUES ("\"1\"", "\"user1\"", "\"user1@domain.com\"", 7, true)`, dbname),
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active) VALUES ("\"2\"", "\"user2\"", "\"user2@domain.com\"", 7, true) WITH singlePK`, dbname),
			args:         []interface{}{"user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES ("\"1\"", "\"user1\"", "\"user3@domain1.com\"", 8, false) WITH single_PK`, dbname),
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES ("\"3\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`, dbname),
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
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) WITH singlePK`, dbname),
			args:         []interface{}{"1", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) WITH single_PK`, dbname),
			args:         []interface{}{"1", "user1", "user2@domain.com", 2, false, nil, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user1", "user2@domain.com", 3, false, nil, "user1"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbl_not_found (id,username,email) VALUES (:1, :2, :3) WITH singlePK`, dbname),
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists_placeholders",
			sql:          `UPSERT INTO db_not_exists.table (id,username,email) VALUES (@1, @2, @3) WITH singlePK`,
			args:         []interface{}{"x", "y", "x", "y"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_num_params",
			sql:       fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false},
			mustError: "expected 6 or 7 input values, got 5",
		},
		{
			name:      "error_invalid_num_params_2",
			sql:       fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user", "app"},
			mustError: "expected 6 or 7 input values, got 8",
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
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' but received %#v", testCase.name, testCase.mustError, err)
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
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, active) VALUES ("\"1\"", "\"user1\"", "\"user1@domain.com\"", 7, true) WITH singlePK`,
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, active) VALUES ("\"2\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`,
			args:         []interface{}{"user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          `UPSERT INTO tbltemp (id,username,email,grade,active) VALUES ("\"1\"", "\"user1\"", "\"user3@domain1.com\"", 8, false)`,
			args:         []interface{}{"user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          `UPSERT INTO tbltemp (id,username,email,grade,active) VALUES ("\"3\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`,
			args:         []interface{}{"user2"},
			mustConflict: true,
		},
		{
			name:         "table_not_exists",
			sql:          `UPSERT INTO tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"") WITH single_PK`,
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
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"1", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) WITH singlePK`,
			args:         []interface{}{"2", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:         []interface{}{"1", "user1", "user2@domain.com", 2, false, nil, "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          `UPSERT INTO tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) WITH single_PK`,
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
			name:      "error_invalid_num_params",
			sql:       `UPSERT INTO tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false},
			mustError: "expected 6 or 7 input values, got 5",
		},
		{
			name:      "error_invalid_num_params_2",
			sql:       `UPSERT INTO tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`,
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user", "app"},
			mustError: "expected 6 or 7 input values, got 8",
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
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' but received %#v", testCase.name, testCase.mustError, err)
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

func TestStmtUpsert_SubPartitions(t *testing.T) {
	testName := "TestStmtUpsert_SubPartitions"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH UK=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active) VALUES ("\"1\"", "\"app\"", "\"user1\"", "\"user1@domain.com\"", 7, true)`, dbname),
			args:         []interface{}{"app", "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active) VALUES ("\"2\"", "\"app\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`, dbname),
			args:         []interface{}{"app", "user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES ("\"1\"", "\"app\"", "\"user1\"", "\"user3@domain1.com\"", 8, false)`, dbname),
			args:         []interface{}{"app", "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES ("\"3\"", "\"app\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`, dbname),
			args:         []interface{}{"app", "user2"},
			mustConflict: true,
		},
		{
			name: "upsert_new_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH uk=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7)`, dbname),
			args:         []interface{}{"1", "app", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "app", "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7)`, dbname),
			args:         []interface{}{"2", "app", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "app", "user2"},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7)`, dbname),
			args:         []interface{}{"1", "app", "user1", "user2@domain.com", 2, false, nil, "app", "user1"},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7)`, dbname),
			args:         []interface{}{"2", "app", "user1", "user2@domain.com", 3, false, nil, "app", "user1"},
			mustConflict: true,
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
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' but received %#v", testCase.name, testCase.mustError, err)
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

func TestStmtUpsert_WithPK(t *testing.T) {
	testName := "TestStmtUpsert_WithPK"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
			name: "upsert_explicit_pk",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH UK=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active) VALUES ("\"1\"", "\"user1\"", "\"user1@domain.com\"", 7, true) WITH pk=/username`, dbname),
			affectedRows: 1,
		},
		{
			name:         "upsert_another_implicit_pk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active) VALUES ("\"2\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`, dbname),
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES ("\"1\"", "\"user1\"", "\"user3@domain1.com\"", 8, false) with PK=/username`, dbname),
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES ("\"3\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`, dbname),
			mustConflict: true,
		},
		{
			name:         "table_not_exists",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`, dbname),
			mustNotFound: true,
		},
		{
			name:         "db_not_exists",
			sql:          `UPSERT INTO db_not_exists.table (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`,
			mustNotFound: true,
		},
		{
			name: "upsert_placeholders_explicit_pk",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) WITH pk=/username`, dbname),
			args:         []interface{}{"1", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders_implicit_pk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) with pk=/username`, dbname),
			args:         []interface{}{"1", "user1", "user2@domain.com", 2, false, nil},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:         []interface{}{"2", "user1", "user2@domain.com", 3, false, nil},
			mustConflict: true,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbl_not_found (id,username,email) VALUES (:1, :2, :3)`, dbname),
			args:         []interface{}{"x", "y", "z"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists_placeholders",
			sql:          `UPSERT INTO db_not_exists.table (id,username,email) VALUES (@1, @2, @3) WITH singlePK`,
			args:         []interface{}{"x", "y", "z"},
			mustNotFound: true,
		},
		{
			name:      "error_invalid_num_params",
			sql:       fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6) WITH pk=/username`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false},
			mustError: "expected 6 or 7 input values, got 5",
		},
		{
			name:      "error_invalid_num_params_2",
			sql:       fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6)`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user", "app"},
			mustError: "expected 6 or 7 input values, got 8",
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
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' but received %#v", testCase.name, testCase.mustError, err)
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

func TestStmtUpsert_WithPK_SubPartitions(t *testing.T) {
	testName := "TestStmtUpsert_WithPK_SubPartitions"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
			name: "upsert_explicit_pk",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH UK=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active) VALUES ("\"1\"", "\"app\"", "\"user1\"", "\"user1@domain.com\"", 7, true) WITH PK=/app,/username`, dbname),
			affectedRows: 1,
		},
		{
			name:         "upsert_another_implicit_pk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active) VALUES ("\"2\"", "\"app\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`, dbname),
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES ("\"1\"", "\"app\"", "\"user1\"", "\"user3@domain1.com\"", 8, false) WITH pk=/app,/username`, dbname),
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES ("\"3\"", "\"app\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`, dbname),
			mustConflict: true,
		},
		{
			name: "upsert_placeholders_explicit_pk",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH uk=/email", dbname),
			},
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7) with pk=/app,/username`, dbname),
			args:         []interface{}{"1", "app", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}},
			affectedRows: 1,
		},
		{
			name:         "upsert_another_placeholders_implicit_pk",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7)`, dbname),
			args:         []interface{}{"2", "app", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}},
			affectedRows: 1,
		},
		{
			name:         "upsert_duplicated_id_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7) with pk=/app,/username`, dbname),
			args:         []interface{}{"1", "app", "user1", "user2@domain.com", 2, false, nil},
			affectedRows: 1,
		},
		{
			name:         "upsert_conflict_uk_placeholders",
			sql:          fmt.Sprintf(`UPSERT INTO %s.tbltemp (id, app, username, email, grade, active, data) VALUES (:1, $2, @3, @4, $5, :6, :7)`, dbname),
			args:         []interface{}{"2", "app", "user1", "user2@domain.com", 3, false, nil},
			mustConflict: true,
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
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' but received %#v", testCase.name, testCase.mustError, err)
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
