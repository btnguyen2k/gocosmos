package gocosmos_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btnguyen2k/gocosmos"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/btnguyen2k/consu/reddo"
)

func TestStmtSelect_Exec(t *testing.T) {
	testName := "TestStmtSelect_Exec"
	db := _openDb(t, testName)
	_, err := db.Exec("SELECT * FROM c WITH db=db WITH collection=table")
	if !errors.Is(err, gocosmos.ErrExecNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

/*----------------------------------------------------------------------*/

func _testSelectPkValueSubPartitions(t *testing.T, testName string, db *sql.DB, collname string) {
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
		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "average"},
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

func TestStmtSelect_Query_PkValue_SubPartitions_SmallRU(t *testing.T) {
	testName := "TestStmtSelect_Query_PkValue_SmallRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSubPartitionsSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectPkValueSubPartitions(t, testName, db, collname)
}

func TestStmtSelect_Query_PkValue_SubPartitions_LargeRU(t *testing.T) {
	testName := "TestStmtSelect_Query_PkValue_LargeRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSubPartitionsLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectPkValueSubPartitions(t, testName, db, collname)
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
		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 AND c.username=:3 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "average"},
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

func TestStmtSelect_Query_PkValue_SmallRU(t *testing.T) {
	testName := "TestStmtSelect_Query_PkValue_SmallRU"
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

func TestStmtSelect_Query_PkValue_LargeRU(t *testing.T) {
	testName := "TestStmtSelect_Query_PkValue_LargeRU"
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

		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category WITH collection=%s WITH cross_partition=true", groupByAggr: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET 1 LIMIT 3 WITH collection=%s WITH cross_partition=true", expectedNumItems: 3, groupByAggr: "average"},
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

func TestStmtSelect_Query_CrossPartition_SmallRU(t *testing.T) {
	testName := "TestStmtSelect_Query_CrossPartition_SmallRU"
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

func TestStmtSelect_Query_CrossPartition_LargeRU(t *testing.T) {
	testName := "TestStmtSelect_Query_CrossPartition_LargeRU"
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

func _testSelectPaging(t *testing.T, testName string, db *sql.DB, collname string, pkranges *gocosmos.RespGetPkranges) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "Simple_OrderAsc", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.id OFFSET :3 LIMIT 23 WITH collection=%s WITH cross_partition=true", maxItemCount: 23, orderField: "id", orderType: reddo.TypeString, orderDirection: "asc"},
		{name: "Simple_OrderDesc", query: "SELECT * FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.id DESC OFFSET :3 LIMIT 29 WITH collection=%s WITH cross_partition=true", maxItemCount: 29, orderField: "id", orderType: reddo.TypeString, orderDirection: "desc"},

		{name: "DistinctDoc_OrderAsc", query: "SELECT DISTINCT c.username FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.username OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, orderField: "username", orderType: reddo.TypeString, orderDirection: "asc", expectedNumItems: numLogicalPartitions, distinctQuery: -1, distinctField: "username"},
		{name: "DistinctValue_OrderDesc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE $1<=c.id AND c.id<@2 ORDER BY c.category DESC OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, orderField: "$1", orderType: reddo.TypeInt, orderDirection: "desc", expectedNumItems: numCategories, distinctQuery: 1, distinctField: "$1"},

		{name: "GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, groupByAggr: "count", expectedNumItems: numCategories},
		{name: "GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, groupByAggr: "sum", expectedNumItems: numCategories},
		{name: "GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, groupByAggr: "min", expectedNumItems: numCategories},
		{name: "GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, groupByAggr: "max", expectedNumItems: numCategories},
		{name: "GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE $1<=c.id AND c.id<@2 GROUP BY c.category OFFSET :3 LIMIT 3 WITH collection=%s WITH cross_partition=true", maxItemCount: 3, groupByAggr: "average", expectedNumItems: numCategories},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			expectedNumItems := high - low
			if testCase.expectedNumItems > 0 {
				expectedNumItems = testCase.expectedNumItems
			}
			sql := fmt.Sprintf(testCase.query, collname)
			offset := 0
			finalRows := make([]map[string]interface{}, 0)
			for {
				params := []interface{}{lowStr, highStr, offset}
				dbRows, err := db.Query(sql, params...)
				if err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
				}
				rows, err := _fetchAllRows(dbRows)
				if err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
				}
				if offset == 0 || len(rows) != 0 {
					_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, 0, rows)
					_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, rows)
					_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, rows)
				}
				if len(rows) == 0 {
					break
				}
				finalRows = append(finalRows, rows...)
				offset += len(rows)
			}
			testCase.maxItemCount = 0
			// {
			// 	for i, row := range finalRows {
			// 		fmt.Printf("%5d: %s\n", i, row["id"])
			// 	}
			// }
			_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, expectedNumItems, finalRows)
			_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, finalRows)
			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, finalRows)
			_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, "", lowStr, highStr, finalRows)
		})
	}
}

func TestStmtSelect_Query_Paging_SmallRU(t *testing.T) {
	testName := "TestStmtSelect_Query_Paging_SmallRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	pkranges := client.GetPkranges(dbname, collname)
	if pkranges.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", pkranges.Error())
	} else if pkranges.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, pkranges.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectPaging(t, testName, db, collname, pkranges)
}

func TestStmtSelect_Query_Paging_LargeRU(t *testing.T) {
	testName := "TestStmtSelect_Query_Paging_LargeRU"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	pkranges := client.GetPkranges(dbname, collname)
	if pkranges.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", pkranges.Error())
	} else if pkranges.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, pkranges.Count)
	}
	db := _openDefaultDb(t, testName, dbname)
	_testSelectPaging(t, testName, db, collname, pkranges)
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
			_ = json.Unmarshal([]byte(testCase.expectedResultJson), &expectedResult)
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
					// fmt.Printf("DEBUG: %#v\n", rows)
					// fmt.Printf("DEBUG: %#v\n", expectedResult)
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
		{name: "QuerySingleDoc", compareField: "id", query: `SELECT * FROM Families f WHERE f.id = "AndersenFamily" WITH collection=%s WITH cross_partition=true`, expectedResultJson: _toJson([]gocosmos.DocInfo{dataMapFamilies["AndersenFamily"]})},
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

func TestStmtSelect_Query_DatasetFamilies_SmallRU(t *testing.T) {
	testName := "TestStmtSelect_Query_DatasetFamilies_SmallRU"
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

func TestStmtSelect_Query_DatasetFamilies_LargeRU(t *testing.T) {
	testName := "TestStmtSelect_Query_DatasetFamilies_LargeRU"
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

func TestStmtSelect_Query_DatasetNutrition_SmallRU(t *testing.T) {
	testName := "TestStmtSelect_Query_DatasetNutrition_SmallRU"
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

func TestStmtSelect_Query_DatasetNutrition_LargeRU(t *testing.T) {
	testName := "TestStmtSelect_Query_DatasetNutrition_LargeRU"
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

func TestStmtSelect_Query(t *testing.T) {
	testName := "TestStmtSelect_Query"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	_, _ = db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	_, _ = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname))
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	if _, err := db.Exec("CREATE COLLECTION dbtemp.tbltemp WITH pk=/username WITH uk=/email"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%02d", i)
		username := "user" + strconv.Itoa(i%4)
		_, _ = db.Exec(fmt.Sprintf("INSERT INTO %s.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", dbname), id, username, "user"+id+"@domain.com", i, username)
	}

	if dbRows, err := db.Query(fmt.Sprintf(`SELECT * FROM c WHERE c.username="user0" AND c.id>"30" ORDER BY c.id WITH database=%s WITH collection=tbltemp`, dbname)); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 17 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 17, len(rows))
		}
		for _, row := range rows {
			id := row["id"].(string)
			if id <= "30" {
				t.Fatalf("%s failed: document #%s should not be returned", testName, id)
			}
		}
	}

	if dbRows, err := db.Query(fmt.Sprintf(`SELECT CROSS PARTITION * FROM tbltemp c WHERE c.username>"user1" AND c.id>"53" WITH database=%s`, dbname)); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 24 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 24, len(rows))
		}
		for _, row := range rows {
			id := row["id"].(string)
			if id <= "53" {
				t.Fatalf("%s failed: document #%s should not be returned", testName, id)
			}
		}
	}

	if _, err := db.Query(fmt.Sprintf(`SELECT * FROM c WITH db=%s WITH collection=tbl_not_found`, dbname)); !errors.Is(err, gocosmos.ErrNotFound) {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", testName, err)
	}

	if _, err := db.Query(`SELECT * FROM c WITH db=db_not_found WITH collection=tbltemp`); !errors.Is(err, gocosmos.ErrNotFound) {
		t.Fatalf("%s failed: expected ErrNotFound but received %#v", testName, err)
	}
}

func TestStmtSelect_Query_SelectLongList(t *testing.T) {
	testName := "TestStmtSelect_Query_SelectLongList"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	_, _ = db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	_, _ = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname))
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	if _, err := db.Exec(fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname)); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%03d", i)
		username := "user" + strconv.Itoa(i%4)
		_, _ = db.Exec(fmt.Sprintf("INSERT INTO %s.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", dbname), id, username, "user"+id+"@domain.com", i, username)
	}

	if dbRows, err := db.Query(fmt.Sprintf(`SELECT * FROM c WHERE c.username="user0" AND c.id>"030" ORDER BY c.id WITH database=%s WITH collection=tbltemp`, dbname)); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 242 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 242, len(rows))
		}
		for _, row := range rows {
			id := row["id"].(string)
			if id <= "030" {
				t.Fatalf("%s failed: document #%s should not be returned", testName, id)
			}
		}
	}
}

func TestStmtSelect_Query_SelectPlaceholder(t *testing.T) {
	testName := "TestStmtSelect_Query_SelectPlaceholder"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	_, _ = db.Exec("DROP DATABASE IF EXISTS db_not_exists")
	_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	_, _ = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname))
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	if _, err := db.Exec(fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname)); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%02d", i)
		username := "user" + strconv.Itoa(i%4)
		_, _ = db.Exec(fmt.Sprintf("INSERT INTO %s.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", dbname), id, username, "user"+id+"@domain.com", i, username)
	}

	if dbRows, err := db.Query(fmt.Sprintf(`SELECT * FROM c WHERE c.username=$2 AND c.id>:1 ORDER BY c.id WITH database=%s WITH collection=tbltemp`, dbname), "30", "user0"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 17 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 17, len(rows))
		}
		for _, row := range rows {
			id := row["id"].(string)
			if id <= "30" {
				t.Fatalf("%s failed: document #%s should not be returned", testName, id)
			}
		}
	}

	if dbRows, err := db.Query(fmt.Sprintf(`SELECT CROSS PARTITION * FROM tbltemp WHERE tbltemp.username>@1 AND tbltemp.grade>:2 WITH database=%s`, dbname), "user1", 53); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 24 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 24, len(rows))
		}
		for _, row := range rows {
			id := row["id"].(string)
			if id <= "53" {
				t.Fatalf("%s failed: document #%s should not be returned", testName, id)
			}
		}
	}

	if _, err := db.Query(fmt.Sprintf(`SELECT * FROM c WHERE c.username=$2 AND c.id>:10 ORDER BY c.id WITH database=%s WITH collection=tbltemp`, dbname), "30", "user0"); err == nil || strings.Index(err.Error(), "no placeholder") < 0 {
		t.Fatalf("%s failed: expecting 'no placeholder' but received %s", testName, err)
	}
}

func TestStmtSelect_Query_SelectPkranges(t *testing.T) {
	testName := "TestStmtSelect_Query_SelectPkranges"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	_, _ = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbname))
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	if _, err := db.Exec(fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname)); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
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
			_, _ = db.Exec(fmt.Sprintf("INSERT INTO %s.tbltemp (id,username,email,grade) VALUES (:1,@2,$3,:4)", dbname), id, username, email, i, username)
			wait.Done()
		}(i)
	}
	wait.Wait()

	query := fmt.Sprintf(`SELECT CROSS PARTITION * FROM c WHERE c.id>$1 ORDER BY c.id OFFSET 5 LIMIT 23 WITH database=%s WITH collection=tbltemp`, dbname)
	if dbRows, err := db.Query(query, "0123"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 23 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 23, len(rows))
		}
		for _, row := range rows {
			id := row["id"].(string)
			if id <= "0123" {
				t.Fatalf("%s failed: document #%s should not be returned", testName, id)
			}
		}
	}

	query = fmt.Sprintf(`SELECT c.username, sum(c.index) FROM tbltemp c WHERE c.id<"0123" GROUP BY c.username OFFSET 110 LIMIT 20 WITH database=%s WITH cross_partition=true`, dbname)
	if dbRows, err := db.Query(query); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		rows, err := _fetchAllRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}

		if len(rows) != 13 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", testName, 13, len(rows))
		}
	}
}
