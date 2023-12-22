package gocosmos_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btnguyen2k/gocosmos"
	"testing"
)

func TestStmtCreateDatabase_Query(t *testing.T) {
	testName := "TestStmtCreateDatabase_Query"
	db := _openDb(t, testName)
	_, err := db.Query("CREATE DATABASE dbtemp")
	if !errors.Is(err, gocosmos.ErrQueryNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtCreateDatabase_Exec(t *testing.T) {
	testName := "TestStmtCreateDatabase_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustConflict bool
		affectedRows int64
	}{
		{
			name:         "create_new",
			initSqls:     []string{fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname)},
			sql:          fmt.Sprintf("CREATE DATABASE %s WITH ru=400", dbname),
			affectedRows: 1,
		},
		{
			name:         "create_conflict",
			sql:          fmt.Sprintf("CREATE DATABASE %s WITH ru=400", dbname),
			mustConflict: true,
		},
		{
			name:         "create_new2",
			initSqls:     []string{fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname)},
			sql:          fmt.Sprintf("CREATE DATABASE %s WITH ru=400", dbname),
			affectedRows: 1,
		},
		{
			name:         "create_if_not_exists",
			sql:          fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s WITH ru=400", dbname),
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
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict {
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

func TestStmtAlterDatabase_Query(t *testing.T) {
	testName := "TestStmtAlterDatabase_Query"
	db := _openDb(t, testName)
	_, err := db.Query("ALTER DATABASE dbtemp WITH ru=400")
	if !errors.Is(err, gocosmos.ErrQueryNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtAlterDatabase_Exec(t *testing.T) {
	testName := "TestStmtAlterDatabase_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "change_ru",
			initSqls:     []string{"DROP DATABASE IF EXISTS db_not_found", fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s WITH ru=400", dbname)},
			sql:          fmt.Sprintf("ALTER DATABASE %s WITH ru=500", dbname),
			affectedRows: 1,
		},
		{
			name:         "change_maxru",
			sql:          fmt.Sprintf("ALTER DATABASE %s WITH maxru=6000", dbname),
			affectedRows: 1,
		},
		{
			name:         "db_not_found",
			sql:          "ALTER DATABASE db_not_found WITH maxru=6000",
			mustNotFound: true,
		},
		{
			name:         "db_no_offer",
			initSqls:     []string{fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname)},
			sql:          fmt.Sprintf("ALTER DATABASE %s WITH maxru=6000", dbname),
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
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound {
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

func TestStmtDropDatabase_Query(t *testing.T) {
	testName := "TestStmtDropDatabase_Query"
	db := _openDb(t, testName)
	_, err := db.Query("DROP DATABASE dbtemp")
	if !errors.Is(err, gocosmos.ErrQueryNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtDropDatabase_Exec(t *testing.T) {
	testName := "TestStmtDropDatabase_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	testData := []struct {
		name         string
		initSqls     []string
		sql          string
		mustNotFound bool
		affectedRows int64
	}{
		{
			name:         "basic",
			initSqls:     []string{fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname)},
			sql:          fmt.Sprintf("DROP DATABASE %s", dbname),
			affectedRows: 1,
		},
		{
			name:         "not_found",
			sql:          fmt.Sprintf("DROP DATABASE %s", dbname),
			mustNotFound: true,
		},
		{
			name:         "basic2",
			initSqls:     []string{fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname)},
			sql:          fmt.Sprintf("DROP DATABASE %s", dbname),
			affectedRows: 1,
		},
		{
			name: "if_exists",
			sql:  fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
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
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound {
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
			if !errors.Is(err, gocosmos.ErrOperationNotSupported) {
				t.Fatalf("%s failed: expected ErrOperationNotSupported but received %#v", testName+"/"+testCase.name, err)
			}
		})
	}
}

func TestStmtListDatabases_Exec(t *testing.T) {
	testName := "TestStmtListDatabases_Exec"
	db := _openDb(t, testName)
	_, err := db.Exec("LIST DATABASES")
	if !errors.Is(err, gocosmos.ErrExecNotSupported) {
		t.Fatalf("%s failed: expected ErrExecNotSupported, but received %#v", testName, err)
	}
}

func TestStmtListDatabases_Query(t *testing.T) {
	testName := "TestStmtListDatabases_Query"
	db := _openDb(t, testName)
	dbnames := []string{"dbtemp", "dbtemp2", "dbtemp1"}
	for _, dbname := range dbnames {
		_, _ = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname))
	}
	defer func() {
		for _, dbname := range dbnames {
			db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
		}
	}()
	dbRows, err := db.Query("LIST DATABASES")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbRows)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch_rows", err)
	}
	ok0, ok1, ok2 := false, false, false
	for _, row := range rows {
		if row["id"] == "dbtemp" {
			ok0 = true
		}
		if row["id"] == "dbtemp1" {
			ok1 = true
		}
		if row["id"] == "dbtemp2" {
			ok2 = true
		}
	}
	if !ok0 {
		t.Fatalf("%s failed: database %s not found", testName, "dbtemp")
	}
	if !ok1 {
		t.Fatalf("%s failed: database %s not found", testName, "dbtemp1")
	}
	if !ok2 {
		t.Fatalf("%s failed: database %s not found", testName, "dbtemp2")
	}
}
