package go_cosmos

import (
	"database/sql"
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

	_, err := db.Exec("CREATE DATABASE dbtemp")
	if err != nil && err != ErrConflict {
		t.Fatalf("%s failed: %s", name, err)
	}

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
