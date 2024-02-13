package gocosmos_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestDriver_invalidConnectionString(t *testing.T) {
	testName := "TestDriver_invalidConnectionString"
	driver := "gocosmos"
	testCases := []struct {
		name string
		dsn  string
	}{
		{"empty", ""},
		{"invalid_endpoint", "AccountEndpoint;AccountKey=demo"},
		{"invalid_key", "AccountEndpoint=demo;AccountKey"},
		{"invalid_key_2", "AccountEndpoint=demo;AccountKey=demo/invalid_key"},
		{"missing_endpoint", "AccountKey=demo"},
		{"missing_key", "AccountEndpoint=demo"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sql.Open(driver, tc.dsn); err == nil {
				t.Fatalf("%s failed: should have error", testName+"/"+tc.name)
			}
		})
	}
}

func TestDriver_Conn(t *testing.T) {
	testName := "TestDriver_Conn"
	db := _openDb(t, testName)
	_, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestDriver_Transaction(t *testing.T) {
	testName := "TestDriver_Transaction"
	db := _openDb(t, testName)
	if tx, err := db.BeginTx(context.Background(), nil); tx != nil || err == nil {
		t.Fatalf("%s failed: transaction is not supported yet", testName)
	} else if strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: transaction is not supported yet / %s", testName, err)
	}
}

func TestDriver_Open(t *testing.T) {
	testName := "TestDriver_Open"
	db := _openDb(t, testName)
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestDriver_Close(t *testing.T) {
	testName := "TestDriver_Close"
	db := _openDb(t, testName)
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}
