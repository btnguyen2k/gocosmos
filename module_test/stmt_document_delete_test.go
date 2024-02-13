package gocosmos_test

import (
	"errors"
	"fmt"
	"github.com/btnguyen2k/gocosmos"
	"strings"
	"testing"
)

func TestStmtDelete_Query(t *testing.T) {
	testName := "TestStmtDelete_Query"
	db := _openDb(t, testName)
	_, err := db.Query("DELETE FROM db.table WHERE id=1", nil)
	if !errors.Is(err, gocosmos.ErrQueryNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtDelete_Exec(t *testing.T) {
	testName := "TestStmtDelete_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil,
				{"1", "user", "user@domain1.com"},
				{"2", "user", "user@domain2.com"},
				{"3", "user", "user@domain3.com"},
				{"4", "user", "user@domain4.com"},
				{"5", "user", "user@domain5.com"},
			},
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="2" with SINGLE_PK`, dbname),
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
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=@1 with SINGLEPK`, dbname),
			args:         []interface{}{"4", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$2`, dbname),
			args:         []interface{}{"dummy", "5", "user"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="\"1\"" with SINGLE_PK`, dbname),
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
			name:      "error_invalid_num_params",
			sql:       fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$6`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false},
			mustError: "expected 6 or 7 input values, got 5",
		},
		{
			name:      "error_invalid_num_params_2",
			sql:       fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$6`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user", "app"},
			mustError: "expected 6 or 7 input values, got 8",
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
					t.Fatalf("%s failed:\nexpected %q\nreceived %q", testCase.name, testCase.mustError, err)
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
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil,
				{"1", "user", "user@domain1.com"},
				{"2", "user", "user@domain2.com"},
				{"3", "user", "user@domain3.com"},
				{"4", "user", "user@domain4.com"},
				{"5", "user", "user@domain5.com"},
			},
			sql:          `DELETE FROM tbltemp WHERE id=1 with SINGLE_PK`,
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          `DELETE FROM tbltemp WHERE id="\"2\""`,
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "delete_3",
			sql:          `DELETE FROM tbltemp WHERE id=:1 with SINGLEPK`,
			args:         []interface{}{"3", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_4",
			sql:          `DELETE FROM tbltemp WHERE id=@1 WITH singlepk=true`,
			args:         []interface{}{"4", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          `DELETE FROM tbltemp WHERE id=$1 with SINGLE_PK=TRUE`,
			args:         []interface{}{"5", "user"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          `DELETE FROM tbltemp WHERE id="\"1\""`,
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
			name:      "error_invalid_num_params",
			sql:       `DELETE FROM tbltemp WHERE id=$6`,
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false},
			mustError: "expected 6 or 7 input values, got 5",
		},
		{
			name:      "error_invalid_num_params_2",
			sql:       `DELETE FROM tbltemp WHERE id=$6`,
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user", "app"},
			mustError: "expected 6 or 7 input values, got 8",
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
		})
	}
}

func TestStmtDelete_SubPartitions(t *testing.T) {
	testName := "TestStmtDelete_SubPartitions"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
			},
			initParams: [][]interface{}{
				nil, nil, nil, nil,
				{"1", "app", "user", "user@domain1.com"},
				{"2", "app", "user", "user@domain2.com"},
				{"3", "app", "user", "user@domain3.com"},
				{"4", "app", "user", "user@domain4.com"},
				{"5", "app", "user", "user@domain5.com"},
			},
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1`, dbname),
			args:         []interface{}{"app", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="\"2\""`, dbname),
			args:         []interface{}{"app", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_3",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=:1`, dbname),
			args:         []interface{}{"3", "app", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_4",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=@2`, dbname),
			args:         []interface{}{"dummy", "4", "app", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$3`, dbname),
			args:         []interface{}{"dummy", "dummy", "5", "app", "user"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1`, dbname),
			args:         []interface{}{"app", "user"},
			affectedRows: 0,
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
		})
	}
}

func TestStmtDelete_WithPK(t *testing.T) {
	testName := "TestStmtDelete_WithPK"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email) VALUES (:1,:2,:3) WITH pk=/username`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil,
				{"1", "user", "user@domain1.com"},
				{"2", "user", "user@domain2.com"},
				{"3", "user", "user@domain3.com"},
				{"4", "user", "user@domain4.com"},
				{"5", "user", "user@domain5.com"},
			},
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1 and username=user`, dbname),
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="2"  and username="\"user\""`, dbname),
			affectedRows: 1,
		},
		{
			name:         "delete_3",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=:1  and username="\"user\""`, dbname),
			args:         []interface{}{"3"},
			affectedRows: 1,
		},
		{
			name:         "delete_4",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=@1  and username=$2`, dbname),
			args:         []interface{}{"4", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$2 and username=:3`, dbname),
			args:         []interface{}{"dummy", "5", "user"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="\"1\"" and user="\"user\""`, dbname),
			affectedRows: 0,
		},
		{
			name:         "table_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.table_not_exists WHERE id=1`, dbname),
			mustNotFound: true,
		},
		{
			name:         "db_not_exists",
			sql:          `DELETE FROM db_not_exists.table WHERE id=1`,
			mustNotFound: true,
		},
		{
			name:      "error_invalid_num_params",
			sql:       fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$6 and username="\"user\""`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false},
			mustError: "expected 6 or 7 input values, got 5",
		},
		{
			name:      "error_invalid_num_params_2",
			sql:       fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$6 and username="\"user\""`, dbname),
			args:      []interface{}{"2", "user", "user@domain1.com", 3, false, nil, "user", "app"},
			mustError: "expected 6 or 7 input values, got 8",
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
					t.Fatalf("%s failed:\nexpected %q\nreceived %q", testCase.name, testCase.mustError, err)
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

func TestStmtDelete_WithPK_SubPartitions(t *testing.T) {
	testName := "TestStmtDelete_WithPK_SubPartitions"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
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
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email) VALUES (:1,:2,:3,:4) WITH pk=/app,/username`, dbname),
			},
			initParams: [][]interface{}{
				nil, nil, nil, nil,
				{"1", "app", "user", "user@domain1.com"},
				{"2", "app", "user", "user@domain2.com"},
				{"3", "app", "user", "user@domain3.com"},
				{"4", "app", "user", "user@domain4.com"},
				{"5", "app", "user", "user@domain5.com"},
			},
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1 AND app=app and username="\"user\""`, dbname),
			affectedRows: 1,
		},
		{
			name:         "delete_2",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id="\"2\"" and app="\"app\"" AND username=user`, dbname),
			affectedRows: 1,
		},
		{
			name:         "delete_3",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=:1 AND app=@2 AND username=:3`, dbname),
			args:         []interface{}{"3", "app", "user"},
			affectedRows: 1,
		},
		{
			name:         "delete_4",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=@2 AND app=$4 and username=user`, dbname),
			args:         []interface{}{"dummy", "4", "dummy", "app"},
			affectedRows: 1,
		},
		{
			name:         "delete_5",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=$3 AND app=:2 AND username=@1`, dbname),
			args:         []interface{}{"user", "app", "5"},
			affectedRows: 1,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`DELETE FROM %s.tbltemp WHERE id=1 AND app=app AND username=user`, dbname),
			affectedRows: 0,
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
		})
	}
}
