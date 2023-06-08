package gocosmos

import (
	"database/sql"
	"os"
	"strings"
	"testing"
)

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
