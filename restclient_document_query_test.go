package gocosmos

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/btnguyen2k/consu/reddo"
)

/*----------------------------------------------------------------------*/

type queryTestCase struct {
	name                  string
	query                 string
	expectedNumItems      int
	maxItemCount          int
	distinctQuery         int // 0=non distinct, 1=distinct values, other: distinct docs
	distinctField         string
	numDistincts          int
	withOrder             bool
	orderDirection        string
	withGroupBy           bool
	groupBy               string
	queryPlanTop          int
	queryPlanOffset       int
	queryPlanLimit        int
	queryPlanDistinctType string
	rewrittenSql          bool
}

func TestRestClient_QueryDocuments_DbOrTableNotExists(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DbOrTableNotExists"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSmallRU(t, testName, client, dbname, collname, 0)
	client.DeleteDatabase("db_not_exists")
	client.DeleteCollection(dbname, "table_not_exists")

	query := QueryReq{DbName: dbname, CollName: collname, Query: "SELECT * FROM c"}

	query.DbName = dbname
	query.CollName = "table_not_exists"
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", testName, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", testName, 404, result.StatusCode)
	}

	query.DbName = "db_not_exists"
	query.CollName = collname
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", testName, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", testName, 404, result.StatusCode)
	}
}

type funcTestFatal func(msg string)

func _verifyDistinct(f funcTestFatal, testName string, testCase queryTestCase, queryResult *RespQueryDocs) {
	if testCase.distinctQuery == 0 {
		return
	}
	distinctSet := make(map[string]bool)
	for _, doc := range queryResult.Documents {
		js, _ := json.Marshal(doc)
		distinctSet[string(js)] = true
	}
	expected := testCase.numDistincts
	if testCase.maxItemCount > 0 {
		expected = testCase.maxItemCount
	}
	if len(distinctSet) != expected {
		f(fmt.Sprintf("%s failed: expected %#v distinct rows, but received %#v", testName, expected, queryResult.Documents))
	}
}

func _verifyOrderBy(f funcTestFatal, testName string, testCase queryTestCase, queryResult *RespQueryDocs) {
	if !testCase.withOrder {
		return
	}
	docList := queryResult.Documents.AsDocInfoSlice()
	if len(docList) == 0 {
		f(fmt.Sprintf("%s failed: empty/invalid query result", testName))
	}
	odir := strings.ToUpper(testCase.orderDirection)
	var prevDoc DocInfo
	for _, doc := range docList {
		if prevDoc != nil {
			pv := prevDoc.GetAttrAsTypeUnsafe("grade", reddo.TypeInt).(int64)
			pc := doc.GetAttrAsTypeUnsafe("grade", reddo.TypeInt).(int64)
			if (odir == "DESC" && pv < pc) || (odir != "DESC" && pv > pc) {
				f(fmt.Sprintf("%s failed: out of order {id: %#v, grade: %#v} -> {id: %#v, grade: %#v}", testName, prevDoc.Id(), pv, doc.Id(), pc))
			}
		}
		prevDoc = doc
	}
}

func _verifyGroupBy(f funcTestFatal, testName string, testCase queryTestCase, partition, lowStr, highStr string, queryResult *RespQueryDocs) {
	if !testCase.withGroupBy {
		return
	}

	countPerCat, sumPerCat := make(map[int]int), make(map[int]int)
	minPerCat, maxPerCat := make(map[int]int), make(map[int]int)
	countPerPartitionPerCat, sumPerPartitionPerCat := make(map[string]map[int]int), make(map[string]map[int]int)
	minPerPartitionPerCat, maxPerPartitionPerCat := make(map[string]map[int]int), make(map[string]map[int]int)
	for i := 0; i < numLogicalPartitions; i++ {
		countPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		sumPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		minPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		maxPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
	}
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			category := docInfo.GetAttrAsTypeUnsafe("category", reddo.TypeInt).(int64)
			grade := docInfo.GetAttrAsTypeUnsafe("grade", reddo.TypeInt).(int64)

			countPerCat[int(category)]++
			sumPerCat[int(category)] += int(grade)
			if minPerCat[int(category)] == 0 || minPerCat[int(category)] > int(grade) {
				minPerCat[int(category)] = int(grade)
			}
			if maxPerCat[int(category)] < int(grade) {
				maxPerCat[int(category)] = int(grade)
			}

			countPerPartitionPerCat[username][int(category)]++
			sumPerPartitionPerCat[username][int(category)] += int(grade)
			if minPerPartitionPerCat[username][int(category)] == 0 || minPerPartitionPerCat[username][int(category)] > int(grade) {
				minPerPartitionPerCat[username][int(category)] = int(grade)
			}
			if maxPerPartitionPerCat[username][int(category)] < int(grade) {
				maxPerPartitionPerCat[username][int(category)] = int(grade)
			}
		}
	}

	docList := queryResult.Documents.AsDocInfoSlice()
	if len(docList) == 0 {
		f(fmt.Sprintf("%s failed: empty/invalid query result", testName))
	}
	for _, doc := range docList {
		category := doc.GetAttrAsTypeUnsafe("Category", reddo.TypeInt).(int64)
		value := doc.GetAttrAsTypeUnsafe("Value", reddo.TypeInt).(int64)
		var expected int
		switch strings.ToUpper(testCase.groupBy) {
		case "COUNT":
			expected = countPerCat[int(category)]
			if partition != "" {
				expected = countPerPartitionPerCat[partition][int(category)]
			}
		case "SUM":
			expected = sumPerCat[int(category)]
			if partition != "" {
				expected = sumPerPartitionPerCat[partition][int(category)]
			}
		case "MIN":
			expected = minPerCat[int(category)]
			if partition != "" {
				expected = minPerPartitionPerCat[partition][int(category)]
			}
		case "MAX":
			expected = maxPerCat[int(category)]
			if partition != "" {
				expected = maxPerPartitionPerCat[partition][int(category)]
			}
		case "AVG", "AVERAGE":
			expected = sumPerCat[int(category)] / countPerCat[int(category)]
			if partition != "" {
				expected = sumPerPartitionPerCat[partition][int(category)] / countPerPartitionPerCat[partition][int(category)]
			}
		default:
			f(fmt.Sprintf("%s failed: <group-by aggregation %#v> expected %#v but received  %#v", testName, testCase.groupBy, expected, value))
		}
		if int(value) != expected {
			f(fmt.Sprintf("%s failed: <group-by aggregation %#v> expected %#v but received  %#v", testName, testCase.groupBy, expected, value))
		}
	}
}

func _countPerPartition(low, high int, dataList []DocInfo) map[string]int {
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	result := make(map[string]int)
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			result[username]++
		}
	}
	return result
}

func _distinctPerPartition(low, high int, dataList []DocInfo, distinctField string) map[string]int {
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	distinctItemsPerPartition := make(map[string]map[string]bool)
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			partitionItems, ok := distinctItemsPerPartition[username]
			if !ok {
				partitionItems = make(map[string]bool)
				distinctItemsPerPartition[username] = partitionItems
			}
			value := docInfo.GetAttrAsTypeUnsafe(distinctField, reddo.TypeString).(string)
			partitionItems[value] = true
		}
	}
	result := make(map[string]int)
	for p := range distinctItemsPerPartition {
		result[p] = len(distinctItemsPerPartition[p])
	}
	return result
}

func _testRestClientQueryDocumentsPkValue(t *testing.T, testName string, client *RestClient, dbname, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	countPerPartition := _countPerPartition(low, high, dataList)
	distinctPerPartition := _distinctPerPartition(low, high, dataList, "category")
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "Limit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: 1, numDistincts: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: -1, numDistincts: numCategories},
		{name: "Limit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "Limit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: -1, maxItemCount: numCategories/2 + 1},
		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", withOrder: true, orderDirection: "asc"},
		{name: "Limit_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 11, withOrder: true, orderDirection: "desc"},
		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "average"},
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, Params: params}
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
			}
			for i := 0; i < numLogicalPartitions; i++ {
				username := "user" + strconv.Itoa(i)
				query.PkValue = username
				expected := testCase.maxItemCount
				if testCase.maxItemCount <= 0 {
					expected = countPerPartition[username]
					if testCase.distinctQuery != 0 {
						testCase.numDistincts = distinctPerPartition[username]
						expected = testCase.numDistincts
					}
				}
				result := client.QueryDocuments(query)
				if result.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/pk="+username, result.Error())
				}
				if !testCase.withGroupBy && (len(result.Documents) != expected || result.Count != expected) {
					t.Fatalf("%s failed: <num-document> expected %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name+"/pk="+username, expected, len(result.Documents), result.Count)
				}
				_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, result)
				_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, result)
				_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, username, lowStr, highStr, result)
			}
		})
	}
}

func TestRestClient_QueryDocuments_PkValue_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_PkValue_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkValue(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_PkValue_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_PkValue_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkValue(t, testName, client, dbname, collname)
}

func _testRestClientQueryDocumentsPkrangeid(t *testing.T, testName string, client *RestClient, dbname, collname string) {
	pkranges := client.GetPkranges(dbname, collname)
	if pkranges.Error() != nil {
		t.Fatalf("%s failed: %s", testName, pkranges.Error())
	}
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "Limit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, numDistincts: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c", distinctQuery: -1, numDistincts: numCategories},
		{name: "Limit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "Limit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c", distinctQuery: -1, maxItemCount: numCategories/2 + 1},
		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", withOrder: true, orderDirection: "asc"},
		{name: "Limit_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 11, withOrder: true, orderDirection: "desc"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1,
				Params: []interface{}{
					map[string]interface{}{"name": "@low", "value": lowStr},
					map[string]interface{}{"name": "@high", "value": highStr},
				},
			}
			totalExpected := high - low
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
				totalExpected = pkranges.Count * testCase.maxItemCount
			} else if testCase.distinctQuery != 0 {
				totalExpected = pkranges.Count * testCase.numDistincts
			}
			totalItems := 0
			for _, pkrange := range pkranges.Pkranges {
				query.PkRangeId = pkrange.Id
				result := client.QueryDocuments(query)
				if result.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, result.Error())
				}
				totalItems += result.Count
				_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, testCase, result)
				_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, testCase, result)
			}
			if !testCase.withGroupBy && totalItems != totalExpected {
				t.Fatalf("%s failed: <total-num-document> expected %#v but received  %#v", testName+"/"+testCase.name, totalExpected, totalItems)
			}
		})
	}
}

func TestRestClient_QueryDocuments_Pkrangeid_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Pkrangeid_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkrangeid(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_Pkrangeid_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Pkrangeid_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkrangeid(t, testName, client, dbname, collname)
}

func _testRestClientQueryDocumentsCrossPartitions(t *testing.T, testName string, client *RestClient, dbname, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "Limit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, numDistincts: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c", distinctQuery: -1, numDistincts: numLogicalPartitions},
		{name: "Limit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "Limit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c", distinctQuery: -1, maxItemCount: numLogicalPartitions/2 + 1},
		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", withOrder: true, orderDirection: "asc"},
		{name: "Limit_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 11, withOrder: true, orderDirection: "desc"},
		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "average"},
		{name: "Limit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/2 + 1, withGroupBy: true, groupBy: "count"},
		// {name: "Limit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/2 + 1, withGroupBy: true, groupBy: "sum"},
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, CrossPartitionEnabled: true, Params: params}
			expected := high - low
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
				expected = testCase.maxItemCount
			} else if testCase.distinctQuery != 0 {
				expected = testCase.numDistincts
			} else if testCase.withGroupBy {
				expected = numCategories
			}
			result := client.QueryDocuments(query)
			if result.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/Query", result.Error())
			}
			if result.Count != expected || len(result.Documents) != expected {
				t.Fatalf("%s failed: <num-document> expected %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name+"/Query", expected, len(result.Documents), result.Count)
			}
			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/Query", testCase, result)
			_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName, testCase, result)
			_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName, testCase, "", lowStr, highStr, result)
		})
	}
}

func TestRestClient_QueryDocuments_CrossPartitions_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_CrossPartitions_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartitions(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_CrossPartitions_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_CrossPartitions_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartitions(t, testName, client, dbname, collname)
}

/*----------------------------------------------------------------------*/

func _testRestClientQueryDocumentsContinuation(t *testing.T, testName string, client *RestClient, dbname, collname string) {
	pkranges := client.GetPkranges(dbname, collname)
	if pkranges.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", pkranges.Error())
	}
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)

	// only "bare" query is working with cross-partition continuation!
	// amongst GROUP BY queries, only count(x) obeys maxItemCount; count(c.field)/sum/min/max/avg do NOT!
	var testCases = []queryTestCase{
		{name: "Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "OrderBy", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 7, withOrder: true, orderDirection: "desc"},
		{name: "DistinctValue", query: "SELECT DISTINCT VALUE c.username FROM c", maxItemCount: 3, distinctQuery: 1, numDistincts: numLogicalPartitions},
		{name: "DistinctDoc", query: "SELECT DISTINCT c.category FROM c", maxItemCount: 3, distinctQuery: -1, numDistincts: numCategories},
		{name: "GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 3, withGroupBy: true, groupBy: "count", numDistincts: numCategories},
		// {name: "GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 3, withGroupBy: true, groupBy: "sum", numDistincts: numCategories},
		// {name: "GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 3, withGroupBy: true, groupBy: "min", numDistincts: numCategories},
		// {name: "GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 3, withGroupBy: true, groupBy: "max", numDistincts: numCategories},
		// {name: "GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 3, withGroupBy: true, groupBy: "average", numDistincts: numCategories},
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: testCase.maxItemCount, CrossPartitionEnabled: true, Params: params}
			expectedNumItems := high - low
			if testCase.distinctQuery != 0 || testCase.withGroupBy {
				expectedNumItems = testCase.numDistincts
			}
			var result *RespQueryDocs
			for {
				tempResult := client.QueryDocuments(query)
				if tempResult.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/Query", tempResult.Error())
				}
				if tempResult.Count > testCase.maxItemCount || len(tempResult.Documents) > testCase.maxItemCount {
					t.Fatalf("%s failed: <num-document> expected not exceeding %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name, testCase.maxItemCount, len(tempResult.Documents), tempResult.Count)
				}
				if result == nil {
					result = tempResult
				} else {
					result.Documents = append(result.Documents, tempResult.Documents...)
					result.Count += tempResult.Count
				}
				query.ContinuationToken = tempResult.ContinuationToken
				if tempResult.ContinuationToken == "" {
					break
				}
			}
			if len(result.Documents) != expectedNumItems || result.Count != expectedNumItems {
				t.Fatalf("%s failed: <num-document> expectedNumItems %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name, expectedNumItems, len(result.Documents), result.Count)
			}
			testCase.maxItemCount = -1
			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
			_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
			_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, "", lowStr, highStr, result)
		})
	}
}

func TestRestClient_QueryDocuments_Continuation_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Continuation_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsContinuation(t, testName, client, dbname, collname)
}

// func TestRestClient_QueryDocuments_Continuation_LargeRU(t *testing.T) {
// 	testName := "TestRestClient_QueryDocuments_Continuation_LargeRU"
// 	client := _newRestClient(t, testName)
// 	dbname := testDb
// 	collname := testTable
// 	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
// 	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
// 	} else if result.Count < 2 {
// 		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
// 	}
// 	_testRestClientQueryDocumentsContinuation(t, testName, client, dbname, collname)
// }

/*----------------------------------------------------------------------*/

type customQueryTestCase struct {
	name, query        string
	expectedResultJson string
	ordering           bool
	nonDocResult       bool
	compareField       string
}

func _testRestClientQueryDocumentsCustomDataset(t *testing.T, testName string, testCases []customQueryTestCase, client *RestClient, dbname, collname string) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, CrossPartitionEnabled: true}
			result := client.QueryDocuments(query)
			if result.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, result.Error())
			}
			var expectedResult []interface{}
			json.Unmarshal([]byte(testCase.expectedResultJson), &expectedResult)
			if result.Count != len(expectedResult) || len(result.Documents) != len(expectedResult) {
				t.Fatalf("%s failed: <num-documents> expected to be %#v but received (count: %#v / len: %#v)", testName+"/"+testCase.name, len(expectedResult), result.Count, len(result.Documents))
			}
			resultDocs := result.Documents
			if !testCase.ordering {
				sort.Slice(resultDocs, func(i, j int) bool {
					if !testCase.nonDocResult {
						var doci, docj = resultDocs.AsDocInfoAt(i), resultDocs.AsDocInfoAt(j)
						stri := doci.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						strj := docj.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						return stri < strj
					}
					stri, _ := json.Marshal(resultDocs[i])
					strj, _ := json.Marshal(resultDocs[j])
					return string(stri) < string(strj)
				})
				sort.Slice(expectedResult, func(i, j int) bool {
					if !testCase.nonDocResult {
						var doci DocInfo = expectedResult[i].(map[string]interface{})
						var docj DocInfo = expectedResult[j].(map[string]interface{})
						stri := doci.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						strj := docj.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						return stri < strj
					}
					stri, _ := json.Marshal(expectedResult[i])
					strj, _ := json.Marshal(expectedResult[j])
					return string(stri) < string(strj)
				})
			}
			for i, doc := range resultDocs {
				myDoc := doc.(interface{})
				if !testCase.nonDocResult {
					docInfo := resultDocs.AsDocInfoAt(i)
					myDoc = docInfo.RemoveSystemAttrs().AsMap()
				}
				expected := expectedResult[i]
				if !reflect.DeepEqual(myDoc, expected) {
					t.Fatalf("%s failed: result\n%#v\ndoes not match expected one\n%#v", testName+"/"+testCase.name, myDoc, expected)
				}
			}
		})
	}
}

func _testRestClientQueryDocumentsDatasetFamilies(t *testing.T, testName string, client *RestClient, dbname, collname string) {
	var testCases = []customQueryTestCase{
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/getting-started
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/from
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/order-by
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/group-by
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/offset-limit
		{name: "QuerySingleDoc", compareField: "id", query: `SELECT * FROM Families f WHERE f.id = "AndersenFamily"`, expectedResultJson: _toJson([]DocInfo{dataMapFamilies["AndersenFamily"]})},
		{name: "QuerySingleAttr", compareField: "id", query: `SELECT f.address FROM Families f WHERE f.id = "AndersenFamily"`, expectedResultJson: `[{"address":{"state":"WA","county":"King","city":"Seattle"}}]`},
		{name: "QuerySubAttrs", compareField: "id", query: `SELECT {"Name":f.id, "City":f.address.city} AS Family FROM Families f WHERE f.address.city = f.address.state`, expectedResultJson: `[{"Family":{"Name":"WakefieldFamily","City":"NY"}}]`},
		{name: "QuerySubItems1", nonDocResult: true, query: `SELECT * FROM Families.children`, expectedResultJson: `[[{"firstName":"Henriette Thaulow","gender":"female","grade":5,"pets":[{"givenName":"Fluffy"}]}],[{"familyName":"Merriam","gender":"female","givenName":"Jesse","grade":1,"pets":[{"givenName":"Goofy"},{"givenName":"Shadow"}]},{"familyName":"Miller","gender":"female","givenName":"Lisa","grade":8}]]`},
		{name: "QuerySubItems2", nonDocResult: true, query: `SELECT * FROM Families.address.state`, expectedResultJson: `["WA","NY"]`},
		{name: "QuerySingleAttrWithOrderBy", compareField: "id", ordering: true, query: `SELECT c.givenName FROM Families f JOIN c IN f.children WHERE f.id = 'WakefieldFamily' ORDER BY f.address.city ASC`, expectedResultJson: `[{"givenName":"Jesse"},{"givenName":"Lisa"}]`},
		{name: "QuerySubAttrsWithOrderByAsc", compareField: "id", ordering: true, query: `SELECT f.id, f.address.city FROM Families f ORDER BY f.address.city`, expectedResultJson: `[{"id":"WakefieldFamily","city":"NY"},{"id":"AndersenFamily","city":"Seattle"}]`},
		{name: "QuerySubAttrsWithOrderByDesc", compareField: "id", ordering: true, query: `SELECT f.id, f.creationDate FROM Families f ORDER BY f.creationDate DESC`, expectedResultJson: `[{"id":"AndersenFamily","creationDate":1431620472},{"id":"WakefieldFamily","creationDate":1431620462}]`},
		{name: "QuerySubAttrsWithOrderByMissingField", compareField: "id", ordering: true, query: `SELECT f.id, f.lastName FROM Families f ORDER BY f.lastName`, expectedResultJson: `[{"id":"WakefieldFamily"},{"id":"AndersenFamily","lastName":"Andersen"}]`},
		{name: "QueryGroupBy", compareField: "id", query: `SELECT COUNT(UniqueLastNames) FROM (SELECT AVG(f.age) FROM f GROUP BY f.lastName) AS UniqueLastNames`, expectedResultJson: `[{"$1":2}]`},
		{name: "QueryOffsetLimitWithOrderBy", compareField: "id", query: `SELECT f.id, f.address.city FROM Families f ORDER BY f.address.city OFFSET 1 LIMIT 1`, expectedResultJson: `[{"id":"AndersenFamily","city":"Seattle"}]`},
		// without ORDER BY, the returned result is un-deterministic
		// {name: "QueryOffsetLimitWithoutOrderBy", query: `SELECT f.id, f.address.city FROM Families f OFFSET 1 LIMIT 1`, expectedResultJson: `[{"id":"AndersenFamily","city":"Seattle"}]`},
	}
	_testRestClientQueryDocumentsCustomDataset(t, testName, testCases, client, dbname, collname)
}

func TestRestClient_QueryDocuments_DatasetFamilies_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DatasetFamilies_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataFamliesSmallRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsDatasetFamilies(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_DatasetFamilies_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DatasetFamilies_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataFamliesLargeRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsDatasetFamilies(t, testName, client, dbname, collname)
}

// func _testRestClientQueryDocumentsDatasetNutrition(t *testing.T, testName string, client *RestClient, dbname, collname string) {
// 	var testCases = []customQueryTestCase{
// 		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/group-by
// 		{name: "Count", query: `SELECT COUNT(1) AS foodGroupCount FROM Food f`, expectedResultJson: `[{"foodGroupCount": 8608}]`},
// 		{name: "QueryGroupBy1", compareField: "foodGroupCount",
// 			query:              "SELECT COUNT(1) AS foodGroupCount, UPPER(f.foodGroup) AS upperFoodGroup FROM Food f GROUP BY UPPER(f.foodGroup)",
// 			expectedResultJson: `[{"foodGroupCount":64,"upperFoodGroup":"SPICES AND HERBS"},{"foodGroupCount":108,"upperFoodGroup":"RESTAURANT FOODS"},{"foodGroupCount":112,"upperFoodGroup":"MEALS, ENTREES, AND SIDE DISHES"},{"foodGroupCount":133,"upperFoodGroup":"NUT AND SEED PRODUCTS"},{"foodGroupCount":165,"upperFoodGroup":"AMERICAN INDIAN/ALASKA NATIVE FOODS"},{"foodGroupCount":171,"upperFoodGroup":"SNACKS"},{"foodGroupCount":183,"upperFoodGroup":"CEREAL GRAINS AND PASTA"},{"foodGroupCount":219,"upperFoodGroup":"FATS AND OILS"},{"foodGroupCount":244,"upperFoodGroup":"SAUSAGES AND LUNCHEON MEATS"},{"foodGroupCount":263,"upperFoodGroup":"DAIRY AND EGG PRODUCTS"},{"foodGroupCount":265,"upperFoodGroup":"FINFISH AND SHELLFISH PRODUCTS"},{"foodGroupCount":315,"upperFoodGroup":"BEVERAGES"},{"foodGroupCount":342,"upperFoodGroup":"PORK PRODUCTS"},{"foodGroupCount":346,"upperFoodGroup":"FRUITS AND FRUIT JUICES"},{"foodGroupCount":346,"upperFoodGroup":"SWEETS"},{"foodGroupCount":362,"upperFoodGroup":"BABY FOODS"},{"foodGroupCount":363,"upperFoodGroup":"BREAKFAST CEREALS"},{"foodGroupCount":370,"upperFoodGroup":"FAST FOODS"},{"foodGroupCount":389,"upperFoodGroup":"LEGUMES AND LEGUME PRODUCTS"},{"foodGroupCount":390,"upperFoodGroup":"POULTRY PRODUCTS"},{"foodGroupCount":438,"upperFoodGroup":"LAMB, VEAL, AND GAME PRODUCTS"},{"foodGroupCount":451,"upperFoodGroup":"SOUPS, SAUCES, AND GRAVIES"},{"foodGroupCount":796,"upperFoodGroup":"BAKED PRODUCTS"},{"foodGroupCount":827,"upperFoodGroup":"VEGETABLES AND VEGETABLE PRODUCTS"},{"foodGroupCount":946,"upperFoodGroup":"BEEF PRODUCTS"}]`},
// 		// TODO
// 		// {name: "QueryGroupBy2", compareField: "foodGroupCount", query: `SELECT COUNT(1) AS foodGroupCount, ARRAY_CONTAINS(f.tags, {name: 'orange'}) AS containsOrangeTag, f.version BETWEEN 0 AND 2 AS correctVersion FROM Food f GROUP BY ARRAY_CONTAINS(f.tags, {name: 'orange'}), f.version BETWEEN 0 AND 2`, expectedResultJson: `[{"foodGroupCount":10,"containsOrangeTag":true,"correctVersion":true},{"foodGroupCount":8598,"containsOrangeTag":false,"correctVersion":true}]`},
// 	}
// 	_testRestClientQueryDocumentsCustomDataset(t, testName, testCases, client, dbname, collname)
// }
//
// func TestRestClient_QueryDocuments_DatasetNutrition_SmallRU(t *testing.T) {
// 	testName := "TestRestClient_QueryDocuments_DatasetNutrition_SmallRU"
// 	client := _newRestClient(t, testName)
// 	dbname := testDb
// 	collname := testTable
// 	_initDataNutritionSmallRU(t, testName, client, dbname, collname)
// 	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
// 	} else if result.Count != 1 {
// 		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
// 	}
// 	_testRestClientQueryDocumentsDatasetNutrition(t, testName, client, dbname, collname)
// }
//
// func TestRestClient_QueryDocuments_DatasetNutrition_LargeRU(t *testing.T) {
// 	testName := "TestRestClient_QueryDocuments_DatasetNutrition_LargeRU"
// 	client := _newRestClient(t, testName)
// 	dbname := testDb
// 	collname := testTable
// 	_initDataNutritionLargeRU(t, testName, client, dbname, collname)
// 	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
// 	} else if result.Count < 2 {
// 		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
// 	}
// 	_testRestClientQueryDocumentsDatasetNutrition(t, testName, client, dbname, collname)
// }
