package gocosmos

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
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

/*----------------------------------------------------------------------*/

func Test_Query_Upsert(t *testing.T) {
	name := "Test_Query_Upsert"
	db := _openDb(t, name)
	_, err := db.Query("UPSERT INTO db.table (a,b,c) VALUES (1,2,3)", nil)
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_Upsert(t *testing.T) {
	name := "Test_Exec_Upsert"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if result, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id, username, email, grade, actived) VALUES ("\"1\"", "\"user1\"", "\"user1@domain.com\"", 7, true)`, "user1"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if regexp.MustCompile(`(?i){\s*LastInsertId\s*:\s*[^}]+?\s*}`).FindString(err.Error()) == "" {
		t.Fatalf("%s failed: can not catch LastInsertId / %s", name, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
	if result, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id, username, email, grade, actived) VALUES ("\"2\"", "\"user2\"", "\"user2@domain.com\"", 7, true)`, "user2"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if regexp.MustCompile(`(?i){\s*LastInsertId\s*:\s*[^}]+?\s*}`).FindString(err.Error()) == "" {
		t.Fatalf("%s failed: can not catch LastInsertId / %s", name, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if result, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id,username,email,grade,actived) VALUES ("\"1\"", "\"user1\"", "\"user1@domain1.com\"", 8, false)`, "user1"); err != nil {
		// duplicated id (in logical partition scope): existing document should be overwritten
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if regexp.MustCompile(`(?i){\s*LastInsertId\s*:\s*[^}]+?\s*}`).FindString(err.Error()) == "" {
		t.Fatalf("%s failed: can not catch LastInsertId / %s", name, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if _, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id,username,email,grade,actived) VALUES ("\"1\"", "\"user2\"", "\"user2@domain.com\"", 9, true)`, "user2"); err != ErrConflict {
		// duplicated unique index (in logical partition scope)
		t.Fatalf("%s failed: expected ErrConflict but received %#v", name, err)
	}

	if _, err := db.Exec(`UPSERT INTO db_not_exists.table (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`, "y"); err != ErrNotFound {
		// database/table not found
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", name, err)
	}
	if _, err := db.Exec(`UPSERT INTO dbtemp.tbl_not_found (id,username,email) VALUES ("\"x\"", "\"y\"", "\"x\"")`, "y"); err != ErrNotFound {
		// database/table not found
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", name, err)
	}
}

func Test_Exec_UpsertPlaceholder(t *testing.T) {
	name := "Test_Exec_UpsertPlaceholder"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if result, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
		"1", "user1", "user1@domain.com", 1, true, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user1"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if regexp.MustCompile(`(?i){\s*LastInsertId\s*:\s*[^}]+?\s*}`).FindString(err.Error()) == "" {
		t.Fatalf("%s failed: can not catch LastInsertId / %s", name, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
	if result, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
		"2", "user2", "user2@domain.com", 2, false, map[string]interface{}{"str": "a string", "num": 1.23, "bool": true, "date": time.Now()}, "user2"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if regexp.MustCompile(`(?i){\s*LastInsertId\s*:\s*[^}]+?\s*}`).FindString(err.Error()) == "" {
		t.Fatalf("%s failed: can not catch LastInsertId / %s", name, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if result, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
		"1", "user1", "user2@domain.com", 2, false, nil, "user1"); err != nil {
		// duplicated id (in logical partition scope): existing document should be overwritten
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if regexp.MustCompile(`(?i){\s*LastInsertId\s*:\s*[^}]+?\s*}`).FindString(err.Error()) == "" {
		t.Fatalf("%s failed: can not catch LastInsertId / %s", name, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if _, err := db.Exec(`UPSERT INTO dbtemp.tbltemp (id, username, email, grade, actived, data) VALUES (:1, $2, @3, @4, $5, :6)`,
		"2", "user1", "user2@domain.com", 3, false, nil, "user1"); err != ErrConflict {
		// duplicated unique index (in logical partition scope)
		t.Fatalf("%s failed: expected ErrConflict but received %#v", name, err)
	}
}

func Test_Query_Delete(t *testing.T) {
	name := "Test_Query_Delete"
	db := _openDb(t, name)
	_, err := db.Query("DELETE FROM db.table WHERE id=1", nil)
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_Delete(t *testing.T) {
	name := "Test_Exec_Delete"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email) VALUES (:1,@2,$3)`, "1", "user", "user@domain1.com", "user")
	db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email) VALUES (:1,@2,$3)`, "2", "user", "user@domain2.com", "user")
	db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email) VALUES (:1,@2,$3)`, "3", "user", "user@domain3.com", "user")
	db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email) VALUES (:1,@2,$3)`, "4", "user", "user@domain4.com", "user")
	db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email) VALUES (:1,@2,$3)`, "5", "user", "user@domain5.com", "user")

	if dbResult, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id=1`, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := dbResult.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := dbResult.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if dbResult, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id="2"`, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := dbResult.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := dbResult.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if dbResult, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id=:1`, "3", "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := dbResult.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := dbResult.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
	if dbResult, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id=@1`, "4", "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := dbResult.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := dbResult.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
	if dbResult, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id=$1`, "5", "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := dbResult.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := dbResult.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if dbResult, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id=1`, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := dbResult.LastInsertId(); id != 0 && err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := dbResult.RowsAffected(); numRows != 0 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=0/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if _, err := db.Exec(`DELETE FROM dbtemp.table_not_exists WHERE id=1`, "user"); err != ErrNotFound {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", name, err)
	}

	if _, err := db.Exec(`DELETE FROM db_not_exists.table WHERE id=1`, "user"); err != ErrNotFound {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", name, err)
	}

	if _, err := db.Exec(`DELETE FROM dbtemp.tbltemp WHERE id=$10`, "1", "user"); err == nil || strings.Index(err.Error(), "invalid value index") < 0 {
		t.Fatalf("%s failed: expected 'invalid value index' bur received %#v", name, err)
	}
}

func Test_Query_Update(t *testing.T) {
	name := "Test_Query_Update"
	db := _openDb(t, name)
	_, err := db.Query("UPDATE db.table SET a=1 WHERE id=2", nil)
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

func Test_Exec_Update(t *testing.T) {
	name := "Test_Exec_Update"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if _, err := db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`,
		"1", "user", "user@domain.com", 1, true, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if _, err := db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`,
		"2", "user", "user2@domain.com", 1, true, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if result, err := db.Exec(`UPDATE dbtemp.tbltemp SET grade=2.0,active=false,data="\"a string 'with' \\\"quote\\\"\"" WHERE id=1`, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if result, err := db.Exec(`UPDATE dbtemp.tbltemp SET username="\"user1\"" WHERE id=1`, "user1"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := result.RowsAffected(); numRows != 0 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=0/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if _, err := db.Exec(`UPDATE dbtemp.tbltemp SET email="\"user2@domain.com\"" WHERE id=1`, "user"); err != ErrConflict {
		t.Fatalf("%s failed: %s", name, err)
	}

	if _, err := db.Exec(`UPDATE dbtemp.tbl_not_found SET email="\"user2@domain.com\"" WHERE id=1`, "user"); err != ErrNotFound {
		t.Fatalf("%s failed: %s", name, err)
	}

	if _, err := db.Exec(`UPDATE db_not_exists.tbltemp SET email="\"user2@domain.com\"" WHERE id=1`, "user"); err != ErrNotFound {
		t.Fatalf("%s failed: %s", name, err)
	}

	// can not change document id
	if result, err := db.Exec(`UPDATE dbtemp.tbltemp SET id="\"0\"" WHERE id=1`, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := result.RowsAffected(); numRows != 0 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=0/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}
}

func Test_Exec_UpdatePlaceholder(t *testing.T) {
	name := "Test_Exec_UpdatePlaceholder"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if _, err := db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`,
		"1", "user", "user@domain.com", 1, true, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if _, err := db.Exec(`INSERT INTO dbtemp.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`,
		"2", "user", "user2@domain.com", 1, true, "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if result, err := db.Exec(`UPDATE dbtemp.tbltemp SET grade=@1,active=$2,data=:3 WHERE id=$4`,
		2.0, false, `a string 'with' "quote"`, "1", "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := result.RowsAffected(); numRows != 1 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=1/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if result, err := db.Exec(`UPDATE dbtemp.tbltemp SET username=:1 WHERE id=$2`,
		"user1", "1", "user1"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := result.RowsAffected(); numRows != 0 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=0/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if _, err := db.Exec(`UPDATE dbtemp.tbltemp SET email=:1 WHERE id=@2`, "user2@domain.com", "1", "user"); err != ErrConflict {
		t.Fatalf("%s failed: %s", name, err)
	}

	if _, err := db.Exec(`UPDATE dbtemp.tbl_not_found SET email=@1 WHERE id=1`, "user2@domain.com", "user"); err != ErrNotFound {
		t.Fatalf("%s failed: %s", name, err)
	}

	if _, err := db.Exec(`UPDATE db_not_exists.tbltemp SET email=$1 WHERE id=1`, "user2@domain.com", "user"); err != ErrNotFound {
		t.Fatalf("%s failed: %s", name, err)
	}

	// can not change document id
	if result, err := db.Exec(`UPDATE dbtemp.tbltemp SET id=:1 WHERE id=1`, "0", "user"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if id, err := result.LastInsertId(); id != 0 || err == nil {
		t.Fatalf("%s failed: expected LastInsertId=0/err!=nil but received LastInsertId=%d/err=%s", name, id, err)
	} else if numRows, err := result.RowsAffected(); numRows != 0 || err != nil {
		t.Fatalf("%s failed: expected RowsAffected=0/err=nil but received RowsAffected=%d/err=%s", name, numRows, err)
	}

	if _, err := db.Exec(`UPDATE dbtemp.tbltemp SET grade=10 WHERE id=$10`, "1", "user"); err == nil || strings.Index(err.Error(), "invalid value index") < 0 {
		t.Fatalf("%s failed: expected 'invalid value index' but received '%s'", name, err)
	}

	if _, err := db.Exec(`UPDATE dbtemp.tbltemp SET grade=$10 WHERE id=1`, "1", "user"); err == nil || strings.Index(err.Error(), "invalid value index") < 0 {
		t.Fatalf("%s failed: expected 'invalid value index' but received '%s'", name, err)
	}
}
