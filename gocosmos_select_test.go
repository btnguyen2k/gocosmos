package gocosmos

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/btnguyen2k/consu/reddo"
)

func Test_Exec_Select(t *testing.T) {
	name := "Test_Exec_Select"
	db := _openDb(t, name)
	_, err := db.Exec("SELECT * FROM c WITH db=db WITH collection=table")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", name, err)
	}
}

/*----------------------------------------------------------------------*/

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

/*----------------------------------------------------------------------*/

func _testSelectPkValue(t *testing.T, testName string, db *sql.DB, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	countPerPartition := _countPerPartition(low, high, dataList)
	distinctPerPartition := _distinctPerPartition(low, high, dataList, "category")
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 WITH collection=%s WITH cross_partition=true"},
		{name: "OffsetLimit_Bare", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 OFFSET 3 LIMIT 5 WITH collection=%s WITH cross_partition=true", expectedNumItems: 5},
		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 ORDER BY c.grade WITH collection=%s WITH cross_partition=true", orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "OffsetLimit_OrderDesc", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 ORDER BY c.category DESC OFFSET 3 LIMIT 5 WITH collection=%s WITH cross_partition=true", expectedNumItems: 5, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc"},

		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 WITH collection=%s WITH cross_partition=true", distinctQuery: 1},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 WITH collection=%s WITH cross_partition=true", distinctQuery: -1},
		{name: "OffsetLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: 1, expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: -1, expectedNumItems: 3},

		{name: "NoLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 ORDER BY c.category WITH collection=%s WITH cross_partition=true", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "$1", orderDirection: "asc"},
		{name: "NoLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 ORDER BY c.category DESC WITH collection=%s WITH cross_partition=true", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc"},
		{name: "OffsetLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 ORDER BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "$1", orderDirection: "asc", expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 ORDER BY c.category DESC OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", expectedNumItems: 3},

		/* GROUP BY with ORDER BY is not supported! */
		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "average"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			savedExpectedNumItems := testCase.expectedNumItems
			for i := 0; i < numLogicalPartitions; i++ {
				testCase.expectedNumItems = savedExpectedNumItems
				expectedNumItems := testCase.expectedNumItems
				username := "user" + strconv.Itoa(i)
				params := []interface{}{lowStr, highStr, username}
				if expectedNumItems <= 0 && testCase.maxItemCount <= 0 {
					expectedNumItems = countPerPartition[username]
					if testCase.distinctQuery != 0 {
						expectedNumItems = distinctPerPartition[username]
					}
					testCase.expectedNumItems = expectedNumItems
				}
				sql := fmt.Sprintf(testCase.query, collname)
				dbRows, err := db.Query(sql, params...)
				if err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
				}
				rows, err := _fetchAllRows(dbRows)
				if err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
				}
				_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, expectedNumItems, rows)
				_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, rows)
				_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, rows)
				_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, username, lowStr, highStr, rows)
			}
		})
	}
}

func TestSelect_PkValue_SmallRU(t *testing.T) {
	testName := "TestSelect_PkValue_SmallRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectPkValue(t, testName, db, collname)
}

func TestSelect_PkValue_LargeRU(t *testing.T) {
	testName := "TestSelect_PkValue_LargeRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectPkValue(t, testName, db, collname)
}

/*----------------------------------------------------------------------*/

func _testSelectCrossPartition(t *testing.T, testName string, db *sql.DB, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 WITH collection=%s WITH cross_partition=true"},
		{name: "OffsetLimit_Bare", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 OFFSET 3 LIMIT 5 WITH collection=%s WITH cross_partition=true", expectedNumItems: 5},
		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.grade WITH collection=%s WITH cross_partition=true", orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "OffsetLimit_OrderDesc", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.category DESC OFFSET 3 LIMIT 5 WITH collection=%s WITH cross_partition=true", expectedNumItems: 5, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc"},

		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 WITH collection=%s WITH cross_partition=true", distinctQuery: 1, expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c WHERE $1<=c.id AND c.id<@2 WITH collection=%s WITH cross_partition=true", distinctQuery: -1, expectedNumItems: numLogicalPartitions},
		{name: "OffsetLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.username FROM c WHERE $1<=c.id AND c.id<@2 OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: 1, expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE $1<=c.id AND c.id<@2 OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: -1, expectedNumItems: 3},

		{name: "NoLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.category WITH collection=%s WITH cross_partition=true", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "$1", orderDirection: "asc", expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.username DESC WITH collection=%s WITH cross_partition=true", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: numLogicalPartitions},
		{name: "OffsetLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "$1", orderDirection: "asc", expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.username DESC OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: 3},

		/* GROUP BY with ORDER BY is not supported! */

		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByField: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByField: "average"},
	}
	params := []interface{}{lowStr, highStr}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			expectedNumItems := high - low
			if testCase.expectedNumItems > 0 {
				expectedNumItems = testCase.expectedNumItems
			}
			sql := fmt.Sprintf(testCase.query, collname)
			dbRows, err := db.Query(sql, params...)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			rows, err := _fetchAllRows(dbRows)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, expectedNumItems, rows)
			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, rows)
			_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, rows)
			_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, "", lowStr, highStr, rows)
		})
	}
}

func TestSelect_CrossPartition_SmallRU(t *testing.T) {
	testName := "TestSelect_CrossPartition_SmallRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectCrossPartition(t, testName, db, collname)
}

func TestSelect_CrossPartition_LargeRU(t *testing.T) {
	testName := "TestSelect_CrossPartition_LargeRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectCrossPartition(t, testName, db, collname)
}

/*----------------------------------------------------------------------*/

func _testSelectCustomDataset(t *testing.T, testName string, testCases []customQueryTestCase, db *sql.DB, collname string) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sql := fmt.Sprintf(testCase.query, collname)
			dbRows, err := db.Query(sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			rows, err := _fetchAllRows(dbRows)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}

			var expectedResult []interface{}
			json.Unmarshal([]byte(testCase.expectedResultJson), &expectedResult)
			if len(rows) != len(expectedResult) {
				t.Fatalf("%s failed: <num-documents> expected to be %#v but received %#v", testName+"/"+testCase.name, len(expectedResult), len(rows))
			}
			if !testCase.ordering {
				sort.Slice(rows, func(i, j int) bool {
					var doci, docj = rows[i], rows[j]
					stri, _ := json.Marshal(doci[testCase.compareField])
					strj, _ := json.Marshal(docj[testCase.compareField])
					return string(stri) < string(strj)
				})
				sort.Slice(expectedResult, func(i, j int) bool {
					var doci, docj = expectedResult[i].(map[string]interface{}), expectedResult[j].(map[string]interface{})
					stri, _ := json.Marshal(doci[testCase.compareField])
					strj, _ := json.Marshal(docj[testCase.compareField])
					return string(stri) < string(strj)
				})
			}
			for i, row := range rows {
				expected := expectedResult[i]
				if !reflect.DeepEqual(row, expected) {
					fmt.Printf("DEBUG: %#v\n", rows)
					fmt.Printf("DEBUG: %#v\n", expectedResult)
					t.Fatalf("%s failed: result\n%#v\ndoes not match expected one\n%#v", testName+"/"+testCase.name, row, expected)
				}
			}
		})
	}
}

func _testSelectDatasetFamilies(t *testing.T, testName string, db *sql.DB, collname string) {
	var testCases = []customQueryTestCase{
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/getting-started
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/from
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/order-by
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/group-by
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/offset-limit
		{name: "QuerySingleDoc", compareField: "id", query: `SELECT * FROM Families f WHERE f.id = "AndersenFamily" WITH collection=%s WITH cross_partition=true`, expectedResultJson: _toJson([]DocInfo{dataMapFamilies["AndersenFamily"]})},
		{name: "QuerySingleAttr", compareField: "id", query: `SELECT f.address FROM Families f WHERE f.id = "AndersenFamily" WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"address":{"state":"WA","county":"King","city":"Seattle"}}]`},
		{name: "QuerySubAttrs", compareField: "id", query: `SELECT {"Name":f.id, "City":f.address.city} AS Family FROM Families f WHERE f.address.city = f.address.state WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"Family":{"Name":"WakefieldFamily","City":"NY"}}]`},
		{name: "QuerySubItems1", compareField: "$1", query: `SELECT * FROM Families.children WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"$1":[{"firstName":"Henriette Thaulow","gender":"female","grade":5,"pets":[{"givenName":"Fluffy"}]}]},{"$1":[{"familyName":"Merriam","gender":"female","givenName":"Jesse","grade":1,"pets":[{"givenName":"Goofy"},{"givenName":"Shadow"}]},{"familyName":"Miller","gender":"female","givenName":"Lisa","grade":8}]}]`},
		{name: "QuerySubItems2", compareField: "$1", query: `SELECT * FROM Families.address.state WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"$1":"WA"},{"$1":"NY"}]`},
		{name: "QuerySingleAttrWithOrderBy", ordering: true, query: `SELECT c.givenName FROM Families f JOIN c IN f.children WHERE f.id = 'WakefieldFamily' ORDER BY f.address.city ASC WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"givenName":"Jesse"},{"givenName":"Lisa"}]`},
		{name: "QuerySubAttrsWithOrderByAsc", ordering: true, query: `SELECT f.id, f.address.city FROM Families f ORDER BY f.address.city WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"id":"WakefieldFamily","city":"NY"},{"id":"AndersenFamily","city":"Seattle"}]`},
		{name: "QuerySubAttrsWithOrderByDesc", ordering: true, query: `SELECT f.id, f.creationDate FROM Families f ORDER BY f.creationDate DESC WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"id":"AndersenFamily","creationDate":1431620472},{"id":"WakefieldFamily","creationDate":1431620462}]`},
		{name: "QuerySubAttrsWithOrderByMissingField", ordering: false, query: `SELECT f.id, f.lastName FROM Families f ORDER BY f.lastName WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"id":"WakefieldFamily","lastName":null},{"id":"AndersenFamily","lastName":"Andersen"}]`},
		{name: "QueryGroupBy", compareField: "$1", query: `SELECT COUNT(UniqueLastNames) FROM (SELECT AVG(f.age) FROM f GROUP BY f.lastName) AS UniqueLastNames WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"$1":2}]`},
		{name: "QueryOffsetLimitWithOrderBy", compareField: "id", query: `SELECT f.id, f.address.city FROM Families f ORDER BY f.address.city OFFSET 1 LIMIT 1 WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"id":"AndersenFamily","city":"Seattle"}]`},
		// without ORDER BY, the returned result is un-deterministic
		// {name: "QueryOffsetLimitWithoutOrderBy", compareField: "id", query: `SELECT f.id, f.address.city FROM Families f OFFSET 1 LIMIT 1 WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"id":"AndersenFamily","city":"Seattle"}]`},
	}
	_testSelectCustomDataset(t, testName, testCases, db, collname)
}

func TestSelect_DatasetFamilies_SmallRU(t *testing.T) {
	testName := "TestSelect_DatasetFamilies_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataFamliesSmallRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectDatasetFamilies(t, testName, db, collname)
}

func TestSelect_DatasetFamilies_LargeRU(t *testing.T) {
	testName := "TestSelect_DatasetFamilies_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataFamliesLargeRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectDatasetFamilies(t, testName, db, collname)
}

func _testSelectDatasetNutrition(t *testing.T, testName string, db *sql.DB, collname string) {
	var testCases = []customQueryTestCase{
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/group-by
		{name: "Count", query: `SELECT COUNT(1) AS foodGroupCount FROM Food f WITH collection=%s WITH cross_partition=true`, expectedResultJson: `[{"foodGroupCount": 8618}]`},
		{name: "QueryGroupBy1", compareField: "foodGroupCount",
			query:              "SELECT COUNT(1) AS foodGroupCount, UPPER(f.foodGroup) AS upperFoodGroup FROM Food f GROUP BY UPPER(f.foodGroup) WITH collection=%s WITH cross_partition=true",
			expectedResultJson: `[{"foodGroupCount":64,"upperFoodGroup":"SPICES AND HERBS"},{"foodGroupCount":108,"upperFoodGroup":"RESTAURANT FOODS"},{"foodGroupCount":113,"upperFoodGroup":"MEALS, ENTREES, AND SIDE DISHES"},{"foodGroupCount":133,"upperFoodGroup":"NUT AND SEED PRODUCTS"},{"foodGroupCount":165,"upperFoodGroup":"AMERICAN INDIAN/ALASKA NATIVE FOODS"},{"foodGroupCount":171,"upperFoodGroup":"SNACKS"},{"foodGroupCount":183,"upperFoodGroup":"CEREAL GRAINS AND PASTA"},{"foodGroupCount":219,"upperFoodGroup":"FATS AND OILS"},{"foodGroupCount":244,"upperFoodGroup":"SAUSAGES AND LUNCHEON MEATS"},{"foodGroupCount":264,"upperFoodGroup":"DAIRY AND EGG PRODUCTS"},{"foodGroupCount":267,"upperFoodGroup":"FINFISH AND SHELLFISH PRODUCTS"},{"foodGroupCount":315,"upperFoodGroup":"BEVERAGES"},{"foodGroupCount":343,"upperFoodGroup":"PORK PRODUCTS"},{"foodGroupCount":346,"upperFoodGroup":"FRUITS AND FRUIT JUICES"},{"foodGroupCount":347,"upperFoodGroup":"SWEETS"},{"foodGroupCount":362,"upperFoodGroup":"BABY FOODS"},{"foodGroupCount":363,"upperFoodGroup":"BREAKFAST CEREALS"},{"foodGroupCount":371,"upperFoodGroup":"FAST FOODS"},{"foodGroupCount":389,"upperFoodGroup":"LEGUMES AND LEGUME PRODUCTS"},{"foodGroupCount":390,"upperFoodGroup":"POULTRY PRODUCTS"},{"foodGroupCount":438,"upperFoodGroup":"LAMB, VEAL, AND GAME PRODUCTS"},{"foodGroupCount":452,"upperFoodGroup":"SOUPS, SAUCES, AND GRAVIES"},{"foodGroupCount":797,"upperFoodGroup":"BAKED PRODUCTS"},{"foodGroupCount":828,"upperFoodGroup":"VEGETABLES AND VEGETABLE PRODUCTS"},{"foodGroupCount":946,"upperFoodGroup":"BEEF PRODUCTS"}]`},
		{name: "QueryGroupBy2", compareField: "foodGroupCount",
			query:              `SELECT COUNT(1) AS foodGroupCount, ARRAY_CONTAINS(f.tags, {name: 'orange'}) AS containsOrangeTag, f.version BETWEEN 0 AND 2 AS correctVersion FROM Food f GROUP BY ARRAY_CONTAINS(f.tags, {name: 'orange'}), f.version BETWEEN 0 AND 2 WITH collection=%s WITH cross_partition=true`,
			expectedResultJson: `[{"foodGroupCount":10,"containsOrangeTag":true,"correctVersion":true},{"foodGroupCount":8608,"containsOrangeTag":false,"correctVersion":true}]`},
	}
	_testSelectCustomDataset(t, testName, testCases, db, collname)
}

func TestSelect_DatasetNutrition_SmallRU(t *testing.T) {
	testName := "TestSelect_DatasetNutrition_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataNutritionSmallRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectDatasetNutrition(t, testName, db, collname)
}

func TestSelect_DatasetNutrition_LargeRU(t *testing.T) {
	testName := "TestSelect_DatasetNutrition_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataNutritionLargeRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectDatasetNutrition(t, testName, db, collname)
}

/*----------------------------------------------------------------------*/

func Test_Query_Select(t *testing.T) {
	name := "Test_Query_Select"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%02d", i)
		username := "user" + strconv.Itoa(i%4)
		db.Exec("INSERT INTO dbtemp.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", id, username, "user"+id+"@domain.com", i, username)
	}

	if dbRows, err := db.Query(`SELECT * FROM c WHERE c.username="user0" AND c.id>"30" ORDER BY c.id WITH database=dbtemp WITH collection=tbltemp`); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 17 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 17, len(rows))
		}
		for k := range rows {
			if k <= "30" {
				t.Fatalf("%s failed: document #%s should not be returned", name, k)
			}
		}
	}

	if dbRows, err := db.Query(`SELECT CROSS PARTITION * FROM tbltemp c WHERE c.username>"user1" AND c.id>"53" WITH database=dbtemp`); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 24 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 24, len(rows))
		}
		for k := range rows {
			if k <= "53" {
				t.Fatalf("%s failed: document #%s should not be returned", name, k)
			}
		}
	}

	if _, err := db.Query(`SELECT * FROM c WITH db=dbtemp WITH collection=tbl_not_found`); err != ErrNotFound {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", name, err)
	}

	if _, err := db.Query(`SELECT * FROM c WITH db=db_not_found WITH collection=tbltemp`); err != ErrNotFound {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", name, err)
	}
}

func Test_Query_SelectLongList(t *testing.T) {
	name := "Test_Query_SelectLongList"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%03d", i)
		username := "user" + strconv.Itoa(i%4)
		db.Exec("INSERT INTO dbtemp.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", id, username, "user"+id+"@domain.com", i, username)
	}

	if dbRows, err := db.Query(`SELECT * FROM c WHERE c.username="user0" AND c.id>"030" ORDER BY c.id WITH database=dbtemp WITH collection=tbltemp`); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 242 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 242, len(rows))
		}
		for k := range rows {
			if k <= "030" {
				t.Fatalf("%s failed: document #%s should not be returned", name, k)
			}
		}
	}
}

func Test_Query_SelectPlaceholder(t *testing.T) {
	name := "Test_Query_SelectPlaceholder"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%02d", i)
		username := "user" + strconv.Itoa(i%4)
		db.Exec("INSERT INTO dbtemp.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", id, username, "user"+id+"@domain.com", i, username)
	}

	if dbRows, err := db.Query(`SELECT * FROM c WHERE c.username=$2 AND c.id>:1 ORDER BY c.id WITH database=dbtemp WITH collection=tbltemp`, "30", "user0"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 17 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 17, len(rows))
		}
		for k := range rows {
			if k <= "30" {
				t.Fatalf("%s failed: document #%s should not be returned", name, k)
			}
		}
	}

	if dbRows, err := db.Query(`SELECT CROSS PARTITION * FROM tbltemp WHERE tbltemp.username>@1 AND tbltemp.grade>:2 WITH database=dbtemp`, "user1", 53); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 24 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 24, len(rows))
		}
		for k := range rows {
			if k <= "53" {
				t.Fatalf("%s failed: document #%s should not be returned", name, k)
			}
		}
	}

	if _, err := db.Query(`SELECT * FROM c WHERE c.username=$2 AND c.id>:10 ORDER BY c.id WITH database=dbtemp WITH collection=tbltemp`, "30", "user0"); err == nil || strings.Index(err.Error(), "no placeholder") < 0 {
		t.Fatalf("%s failed: expecting 'no placeholder' but received %s", name, err)
	}
}

func Test_Query_SelectPkranges(t *testing.T) {
	name := "Test_Query_SelectPkranges"
	db := _openDb(t, name)

	db.Exec("DROP DATABASE IF EXISTS dbtemp")
	db.Exec("CREATE DATABASE IF NOT EXISTS dbtemp")
	defer db.Exec("DROP DATABASE IF EXISTS dbtemp")
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	var wait sync.WaitGroup
	n := 1000
	d := 256
	wait.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			id := fmt.Sprintf("%04d", i)
			username := "user" + fmt.Sprintf("%02x", i%d)
			email := "user" + strconv.Itoa(i) + "@domain.com"
			db.Exec("INSERT INTO dbtemp.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", id, username, email, i, username)
			wait.Done()
		}(i)
	}
	wait.Wait()

	query := `SELECT CROSS PARTITION * FROM c WHERE c.id>$1 ORDER BY c.id OFFSET 5 LIMIT 23 WITH database=dbtemp WITH collection=tbltemp`
	if dbRows, err := db.Query(query, "0123"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 23 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 23, len(rows))
		}
		for k := range rows {
			if k <= "0123" {
				t.Fatalf("%s failed: document #%s should not be returned", name, k)
			}
		}
	}

	query = `SELECT c.username, sum(c.index) FROM tbltemp c WHERE c.id<"0123" GROUP BY c.username OFFSET 110 LIMIT 20 WITH database=dbtemp WITH cross_partition=true`
	if dbRows, err := db.Query(query); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		colTypes, err := dbRows.ColumnTypes()
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		numCols := len(colTypes)
		rows := make(map[string]map[string]interface{})
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
				id := fmt.Sprintf("%s", row["username"])
				rows[id] = row
			} else if err != sql.ErrNoRows {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
		if len(rows) != 13 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 13, len(rows))
		}
	}
}
