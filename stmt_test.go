package gocosmos

import (
	"database/sql"
	"testing"
)

func _fetchAllRows(dbRows *sql.Rows) ([]map[string]interface{}, error) {
	colTypes, err := dbRows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	numCols := len(colTypes)
	rows := make([]map[string]interface{}, 0)
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
			rows = append(rows, row)
		} else if err != sql.ErrNoRows {
			return nil, err
		}
	}
	return rows, nil
}

func TestStmt_NumInput(t *testing.T) {
	name := "TestStmt_NumInput"
	testData := map[string]int{
		"CREATE DATABASE dbtemp":               0,
		"DROP DATABASE dbtemp":                 0,
		"CREATE DATABASE IF NOT EXISTS dbtemp": 0,
		"DROP DATABASE IF EXISTS dbtemp":       0,

		"CREATE TABLE db.tbltemp WITH pk=/id":                    0,
		"DROP TABLE db.tbltemp":                                  0,
		"CREATE TABLE IF NOT EXISTS db.tbltemp WITH pk=/id":      0,
		"DROP TABLE IF EXISTS db.tbltemp":                        0,
		"CREATE COLLECTION db.tbltemp WITH pk=/id":               0,
		"DROP COLLECTION db.tbltemp":                             0,
		"CREATE COLLECTION IF NOT EXISTS db.tbltemp WITH pk=/id": 0,
		"DROP COLLECTION IF EXISTS db.tbltemp":                   0,

		"SELECT * FROM tbltemp WHERE id=@1 AND email=$2 OR username=:3 WITH db=mydb": 3,
		"INSERT INTO db.tbltemp (id, name, email) VALUES ($1, :2, @3)":               3 + 1, // need one extra input for partition key
		"DELETE FROM db.tbltemp WHERE id=$1":                                         1 + 1, // need one extra input for partition key
	}

	for query, numInput := range testData {
		if stmt, err := parseQuery(nil, query); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+query, err)
		} else if v := stmt.NumInput(); v != numInput {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/"+query, numInput, v)
		}
	}
}
