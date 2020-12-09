package go_cosmos

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
)

func Test_OpenDatabase(t *testing.T) {
	name := "Test_OpenDatabase"
	driver := "gocosmos"
	dsn := "dummy"
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if db == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestDriver_invalidConnectionString(t *testing.T) {
	name := "TestDriver_invalidConnectionString"
	driver := "gocosmos"
	{
		db, _ := sql.Open(driver, "AccountEndpoint;AccountKey=demo")
		if err := db.Ping(); err == nil {
			t.Fatalf("%s failed: should have error", name)
		}
	}
	{
		db, _ := sql.Open(driver, "AccountEndpoint=demo;AccountKey")
		if err := db.Ping(); err == nil {
			t.Fatalf("%s failed: should have error", name)
		}
	}
	{
		db, _ := sql.Open(driver, "AccountEndpoint=demo;AccountKey=demo/invalid_key")
		if err := db.Ping(); err == nil {
			t.Fatalf("%s failed: should have error", name)
		}
	}
}

func TestDriver_missingEndpoint(t *testing.T) {
	name := "TestDriver_missingEndpoint"
	driver := "gocosmos"
	dsn := "AccountKey=demo"
	db, _ := sql.Open(driver, dsn)
	if err := db.Ping(); err == nil {
		t.Fatalf("%s failed: should have error", name)
	}
}

func TestDriver_missingAccountKey(t *testing.T) {
	name := "TestDriver_missingAccountKey"
	driver := "gocosmos"
	dsn := "AccountEndpoint=demo"
	db, _ := sql.Open(driver, dsn)
	if err := db.Ping(); err == nil {
		t.Fatalf("%s failed: should have error", name)
	}
}

func _openDb(t *testing.T, testName string) *sql.DB {
	driver := "gocosmos"
	url := strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, "")
	if url == "" {
		t.Skipf("%s skipped", testName)
	}
	db, err := sql.Open(driver, url)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/sql.Open", err)
	}
	return db
}

func TestDriver_Open(t *testing.T) {
	name := "TestDriver_Open"
	db := _openDb(t, name)
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestDriver_Close(t *testing.T) {
	name := "TestDriver_Close"
	db := _openDb(t, name)
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

/*----------------------------------------------------------------------*/

func Test_Query_CreateDatabase(t *testing.T) {
	name := "Test_Query_CreateDatabase"
	db := _openDb(t, name)
	_, err := db.Query("CREATE DATABASE dbtemp")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_CreateDatabase(t *testing.T) {
	name := "Test_Query_CreateDatabase"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS dbtemp")

	result, err := db.Exec("CREATE DATABASE dbtemp WITH ru=400")
	if err != nil && err != ErrConflict {
		t.Fatalf("%s failed: %s", name, err)
	}
	if id, err := result.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	}
	if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp WITH maxru=4000")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if id, err := result.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	}
	if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
}

func Test_Query_DropDatabase(t *testing.T) {
	name := "Test_Query_DropDatabase"
	db := _openDb(t, name)
	_, err := db.Query("DROP DATABASE dbtemp")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_DropDatabase(t *testing.T) {
	name := "Test_Exec_DropDatabase"
	db := _openDb(t, name)

	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")

	_, err := db.Exec("DROP DATABASE dbtemp")
	if err != nil && err != ErrNotFound {
		t.Fatalf("%s failed: %s", name, err)
	}

	_, err = db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func Test_Exec_ListDatabases(t *testing.T) {
	name := "Test_Exec_ListDatabases"
	db := _openDb(t, name)
	_, err := db.Exec("LIST DATABASES")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Query_ListDatabases(t *testing.T) {
	name := "Test_Query_ListDatabases"
	db := _openDb(t, name)

	db.Exec("CREATE DATABASE dbtemp")
	db.Exec("CREATE DATABASE dbtemp2")
	db.Exec("CREATE DATABASE dbtemp1")

	dbRows, err := db.Query("LIST DATABASES")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	colTypes, err := dbRows.ColumnTypes()
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	numCols := len(colTypes)
	result := make(map[string]map[string]interface{})
	for dbRows.Next() {
		vals := make([]interface{}, numCols)
		scanVals := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			scanVals[i] = &vals[i]
		}
		if err := dbRows.Scan(scanVals...); err == nil {
			row := make(map[string]interface{})
			for i, v := range colTypes {
				row[v.Name()] = vals[i]
			}
			id := fmt.Sprintf("%s", row["id"])
			result[id] = row
		} else if err != sql.ErrNoRows {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
	_, ok1 := result["dbtemp"]
	_, ok2 := result["dbtemp1"]
	_, ok3 := result["dbtemp2"]
	if !ok1 {
		t.Fatalf("%s failed: database %s not found", name, "dbtemp")
	}
	if !ok2 {
		t.Fatalf("%s failed: database %s not found", name, "dbtemp1")
	}
	if !ok3 {
		t.Fatalf("%s failed: database %s not found", name, "dbtemp2")
	}
}

/*----------------------------------------------------------------------*/

func Test_Query_CreateCollection(t *testing.T) {
	name := "Test_Query_CreateCollection"
	db := _openDb(t, name)
	_, err := db.Query("CREATE COLLECTION dbtemp.tbltemp WITH pk=/id")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_CreateCollection(t *testing.T) {
	name := "Test_Exec_CreateCollection"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")

	result, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/id WITH ru=400")
	if err != nil && err != ErrConflict {
		t.Fatalf("%s failed: %s", name, err)
	}
	if id, err := result.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	}
	if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS dbtemp.tbltemp1 WITH largepk=/a/b/c WITH maxru=4000 WITH uk=/a;/b,/c/d")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if id, err := result.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	}
	if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
}

func Test_Query_DropCollection(t *testing.T) {
	name := "Test_Query_DropCollection"
	db := _openDb(t, name)
	_, err := db.Query("DROP COLLECTION dbtemp.tbltemp")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_DropCollection(t *testing.T) {
	name := "Test_Exec_DropCollection"
	db := _openDb(t, name)

	db.Exec("CREATE COLLECTION IF NOT EXISTS dbtemp.tbltemp WITH pk=/id")

	_, err := db.Exec("DROP COLLECTION dbtemp.tbltemp")
	if err != nil && err != ErrNotFound {
		t.Fatalf("%s failed: %s", name, err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS dbtemp.tbltemp")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func Test_Exec_ListCollections(t *testing.T) {
	name := "Test_Exec_ListCollections"
	db := _openDb(t, name)
	_, err := db.Exec("LIST COLLECTIONS FROM dbtemp")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Query_ListCollections(t *testing.T) {
	name := "Test_Query_ListCollections"
	db := _openDb(t, name)

	db.Exec("CREATE DATABASE dbtemp")
	db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/a")
	db.Exec("CREATE TABLE dbtemp.tbltemp2 WITH pk=/b")
	db.Exec("CREATE COLLECTION dbtemp.tbltemp1 WITH pk=/c")

	dbRows, err := db.Query("LIST COLLECTIONS FROM dbtemp")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	colTypes, err := dbRows.ColumnTypes()
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	numCols := len(colTypes)
	result := make(map[string]map[string]interface{})
	for dbRows.Next() {
		vals := make([]interface{}, numCols)
		scanVals := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			scanVals[i] = &vals[i]
		}
		if err := dbRows.Scan(scanVals...); err == nil {
			row := make(map[string]interface{})
			for i, v := range colTypes {
				row[v.Name()] = vals[i]
			}
			id := fmt.Sprintf("%s", row["id"])
			result[id] = row
		} else if err != sql.ErrNoRows {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
	_, ok1 := result["tbltemp"]
	_, ok2 := result["tbltemp1"]
	_, ok3 := result["tbltemp2"]
	if !ok1 {
		t.Fatalf("%s failed: collection %s not found", name, "dbtemp.tbltemp")
	}
	if !ok2 {
		t.Fatalf("%s failed: collection %s not found", name, "dbtemp.tbltemp1")
	}
	if !ok3 {
		t.Fatalf("%s failed: collection %s not found", name, "dbtemp.tbltemp2")
	}
}
