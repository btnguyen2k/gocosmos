package gocosmos

import (
	"context"
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

func _openDefaultDb(t *testing.T, testName, defaultDb string) *sql.DB {
	driver := "gocosmos"
	url := strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, "")
	if url == "" {
		t.Skipf("%s skipped", testName)
	}
	if defaultDb != "" {
		if strings.Index(url, "DefaultDb=") < 0 {
			url += ";DefaultDb=" + defaultDb
		}
	}
	db, err := sql.Open(driver, url)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/sql.Open", err)
	}
	return db
}

func _openDb(t *testing.T, testName string) *sql.DB {
	return _openDefaultDb(t, testName, "")
}

func TestDriver_Conn(t *testing.T) {
	name := "TestDriver_Conn"
	db := _openDb(t, name)
	_, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestDriver_Transaction(t *testing.T) {
	name := "TestDriver_Transaction"
	db := _openDb(t, name)
	if tx, err := db.BeginTx(context.Background(), nil); tx != nil || err == nil {
		t.Fatalf("%s failed: transaction is not supported yet", name)
	} else if strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: transaction is not supported yet / %s", name, err)
	}
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
