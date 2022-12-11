package gocosmos

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/btnguyen2k/consu/reddo"
)

/*----------------------------------------------------------------------*/

type queryTestCase struct {
	name                  string
	query                 string
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
	dbname := "mydb"
	collname := "mytable"
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
	dbname := "mydb"
	collname := "mytable"
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
	dbname := "mydb"
	collname := "mytable"
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
	dbname := "mydb"
	collname := "mytable"
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
	dbname := "mydb"
	collname := "mytable"
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
	dbname := "mydb"
	collname := "mytable"
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
	dbname := "mydb"
	collname := "mytable"
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartitions(t, testName, client, dbname, collname)
}

/*----------------------------------------------------------------------*/

// func _testRestClientQueryDocumentsContinuation(t *testing.T, testName string, client *RestClient, dbname, collname string) {
// 	pkranges := client.GetPkranges(dbname, collname)
// 	if pkranges.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", pkranges.Error())
// 	}
// 	low, high := 123, 987
// 	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
//
// 	/*
// 		which does not work with cross-partition continuation:
// 		- ORDER BY + maxItemCount
// 	*/
// 	var testCases = []queryTestCase{
// 		{name: "Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
// 		{name: "DistinctValue", query: "SELECT DISTINCT VALUE c.username FROM c", maxItemCount: 3, distinctQuery: 1, numDistincts: numLogicalPartitions},
// 		{name: "DistinctDoc", query: "SELECT DISTINCT c.category FROM c", maxItemCount: 3, distinctQuery: -1, numDistincts: numCategories},
// 		// {name: "GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 3, withGroupBy: true, groupBy: "count"},
// 		// {name: "GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "sum"},
// 		// {name: "GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "min"},
// 		// {name: "GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "max"},
// 		// {name: "GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", withGroupBy: true, groupBy: "average"},
// 	}
// 	for _, testCase := range testCases {
// 		t.Run(testCase.name, func(t *testing.T) {
// 			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: testCase.maxItemCount, CrossPartitionEnabled: true,
// 				Params: []interface{}{
// 					map[string]interface{}{"name": "@low", "value": lowStr},
// 					map[string]interface{}{"name": "@high", "value": highStr},
// 				},
// 			}
// 			expectedNumItems := high - low
// 			if testCase.distinctQuery != 0 {
// 				expectedNumItems = testCase.numDistincts
// 			}
// 			var result *RespQueryDocs
// 			for {
// 				tempResult := client.QueryDocuments(query)
// 				if tempResult.Error() != nil {
// 					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/Query", tempResult.Error())
// 				}
// 				if tempResult.Count > testCase.maxItemCount*pkranges.Count || len(tempResult.Documents) > testCase.maxItemCount*pkranges.Count {
// 					t.Fatalf("%s failed: <num-document> expected not exceeding %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name, testCase.maxItemCount*pkranges.Count, len(tempResult.Documents), tempResult.Count)
// 				}
// 				if result == nil {
// 					result = tempResult
// 				} else {
// 					result.Documents = append(result.Documents, tempResult.Documents...)
// 					result.Count += tempResult.Count
// 				}
// 				query.ContinuationToken = tempResult.ContinuationToken
// 				if tempResult.ContinuationToken == "" {
// 					break
// 				}
// 			}
// 			if len(result.Documents) != expectedNumItems {
// 				t.Fatalf("%s failed: <num-document> expectedNumItems %#v but received %#v", testName+"/"+testCase.name, expectedNumItems, len(result.Documents))
// 			}
// 			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
// 			_verifyOrderBy(t, testName+"/"+testCase.name, testCase, result)
// 			// _verifyGroupBy(t, testName, testCase, "", lowStr, highStr, result)
// 		})
// 	}
// }
//
// func TestRestClient_QueryDocuments_Continuation_SmallRU(t *testing.T) {
// 	testName := "TestRestClient_QueryDocuments_Continuation_SmallRU"
// 	client := _newRestClient(t, testName)
// 	dbname := "mydb"
// 	collname := "mytable"
// 	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
// 	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
// 	} else if result.Count != 1 {
// 		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
// 	}
// 	_testRestClientQueryDocumentsContinuation(t, testName, client, dbname, collname)
// }
//
// func TestRestClient_QueryDocuments_Continuation_LargeRU(t *testing.T) {
// 	testName := "TestRestClient_QueryDocuments_Continuation_LargeRU"
// 	client := _newRestClient(t, testName)
// 	dbname := "mydb"
// 	collname := "mytable"
// 	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
// 	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
// 	} else if result.Count < 2 {
// 		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
// 	}
// 	_testRestClientQueryDocumentsContinuation(t, testName, client, dbname, collname)
// }

// func TestRestClient_QueryAllDocuments(t *testing.T) {
// 	name := "TestRestClient_QueryDocuments"
// 	client := _newRestClient(t, name)
//
// 	dbname := "mydb"
// 	collname := "mytable"
// 	client.DeleteDatabase(dbname)
// 	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
// 	client.CreateCollection(CollectionSpec{
// 		DbName:           dbname,
// 		CollName:         collname,
// 		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
// 		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
// 	})
// 	totalRu := 0.0
// 	var sessionToken string
// 	for i := 0; i < 100; i++ {
// 		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user", "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
// 		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		} else {
// 			totalRu += result.RequestCharge
// 			sessionToken = result.SessionToken
// 		}
// 	}
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)
//
// 	query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, ConsistencyLevel: "Session", SessionToken: sessionToken,
// 		Query:                 "SELECT * FROM c",
// 		CrossPartitionEnabled: true,
// 	}
// 	var result *RespQueryDocs
// 	documents := make([]DocInfo, 0)
// 	totalRu = 0.0
// 	for result = client.QueryDocuments(query); result.Error() == nil; {
// 		totalRu += result.RequestCharge
// 		documents = append(documents, result.Documents...)
// 		if result.ContinuationToken == "" {
// 			break
// 		}
// 		query.ContinuationToken = result.ContinuationToken
// 		result = client.QueryDocuments(query)
// 	}
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
// 	if result.Error() != nil {
// 		t.Fatalf("%s failed: %s", name, result.Error())
// 	}
// 	if len(documents) != 100 {
// 		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 63, len(documents))
// 	}
//
// 	query.DbName = dbname
// 	query.CollName = "table_not_found"
// 	if result := client.QueryDocuments(query); result.CallErr != nil {
// 		t.Fatalf("%s failed: %s", name, result.CallErr)
// 	} else if result.StatusCode != 404 {
// 		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
// 	}
//
// 	client.DeleteDatabase("db_not_found")
// 	query.DbName = "db_not_found"
// 	query.CollName = collname
// 	if result := client.QueryDocuments(query); result.CallErr != nil {
// 		t.Fatalf("%s failed: %s", name, result.CallErr)
// 	} else if result.StatusCode != 404 {
// 		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
// 	}
// }
//
// func TestRestClient_QueryDocumentsPkranges(t *testing.T) {
// 	name := "TestRestClient_QueryDocumentsPkranges"
// 	client := _newRestClient(t, name)
//
// 	dbname := "mydb"
// 	collname := "mytable"
// 	client.DeleteDatabase(dbname)
// 	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
// 	client.CreateCollection(CollectionSpec{
// 		DbName:           dbname,
// 		CollName:         collname,
// 		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
// 		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
// 	})
// 	totalRu := 0.0
// 	var wait sync.WaitGroup
// 	n := 100
// 	d := 16
// 	wait.Add(n)
// 	for i := 0; i < n; i++ {
// 		go func(i int) {
// 			id := fmt.Sprintf("%04d", i)
// 			username := "user" + fmt.Sprintf("%02x", i%d)
// 			email := "user" + strconv.Itoa(i) + "@domain.com"
// 			if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname,
// 				PartitionKeyValues: []interface{}{username},
// 				DocumentData:       map[string]interface{}{"id": id, "username": username, "email": email, "index": i},
// 			}); result.Error() != nil {
// 				t.Fatalf("%s failed: %s", name, result.Error())
// 			} else {
// 				totalRu += result.RequestCharge
// 			}
// 			wait.Done()
// 		}(i)
// 	}
// 	wait.Wait()
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)
//
// 	{
// 		query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, CrossPartitionEnabled: true,
// 			Query:  "SELECT * FROM c WHERE c.id>=@id ORDER BY c.id OFFSET 5 LIMIT 3",
// 			Params: []interface{}{map[string]interface{}{"name": "@id", "value": "0037"}},
// 		}
// 		var result *RespQueryDocs
// 		documents := make([]DocInfo, 0)
// 		totalRu = 0.0
// 		for result = client.QueryDocuments(query); result.Error() == nil; {
// 			totalRu += result.RequestCharge
// 			documents = append(documents, result.Documents...)
// 			if result.ContinuationToken == "" {
// 				break
// 			}
// 			query.ContinuationToken = result.ContinuationToken
// 			result = client.QueryDocuments(query)
// 		}
// 		fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
// 		if result.Error() != nil {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		}
// 		if len(documents) != 3 {
// 			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 3, len(documents))
// 		}
// 		if documents[0].Id() != "0042" || documents[1].Id() != "0043" || documents[2].Id() != "0044" {
// 			t.Fatalf("%s failed: <documents> not in correct order", name)
// 		}
// 	}
//
// 	{
// 		query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, CrossPartitionEnabled: true,
// 			Query:  "SELECT c.username, sum(c.index) FROM c WHERE c.id<@id GROUP BY c.username",
// 			Params: []interface{}{map[string]interface{}{"name": "@id", "value": "0123"}},
// 		}
// 		var result *RespQueryDocs
// 		documents := make([]DocInfo, 0)
// 		totalRu = 0.0
// 		for result = client.QueryDocuments(query); result.Error() == nil; {
// 			totalRu += result.RequestCharge
// 			documents = append(documents, result.Documents...)
// 			if result.ContinuationToken == "" {
// 				break
// 			}
// 			query.ContinuationToken = result.ContinuationToken
// 			result = client.QueryDocuments(query)
// 		}
// 		fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
// 		if result.Error() != nil {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		}
// 		if len(documents) != d {
// 			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, d, len(documents))
// 		}
// 	}
// }
//
// func TestRestClient_ListDocuments(t *testing.T) {
// 	name := "TestRestClient_ListDocuments"
// 	client := _newRestClient(t, name)
//
// 	dbname := "mydb"
// 	collname := "mytable"
// 	client.DeleteDatabase(dbname)
// 	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
// 	client.CreateCollection(CollectionSpec{
// 		DbName:           dbname,
// 		CollName:         collname,
// 		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
// 		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
// 	})
// 	totalRu := 0.0
//
// 	// if result := client.GetCollection(dbname, collname); result.Error() != nil {
// 	// 	t.Fatalf("%s failed: %s", name, result.Error())
// 	// } else {
// 	// 	fmt.Println("\tCollection etag:", result.Etag, result.Ts)
// 	// }
//
// 	for i := 0; i < 100; i++ {
// 		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user", "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
// 		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		} else {
// 			totalRu += result.RequestCharge
// 		}
// 	}
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)
//
// 	// var collEtag string
// 	// if result := client.GetCollection(dbname, collname); result.Error() != nil {
// 	// 	t.Fatalf("%s failed: %s", name, result.Error())
// 	// } else {
// 	// 	collEtag = result.Etag
// 	// 	fmt.Println("\tCollection etag:", result.Etag, result.Ts)
// 	// }
//
// 	var sessionToken string
// 	rand.Seed(time.Now().UnixNano())
// 	removed := make(map[int]bool)
// 	for i := 0; i < 5; i++ {
// 		id := rand.Intn(100)
// 		removed[id] = true
// 		result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: fmt.Sprintf("%02d", id), PartitionKeyValues: []interface{}{"user"}})
// 		if result.Error() != nil && result.StatusCode != 404 {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		} else {
// 			sessionToken = result.SessionToken
// 		}
//
// 		id = rand.Intn(100)
// 		if !removed[id] {
// 			doc := DocumentSpec{
// 				DbName:             dbname,
// 				CollName:           collname,
// 				IsUpsert:           true,
// 				PartitionKeyValues: []interface{}{"user"},
// 				DocumentData:       map[string]interface{}{"id": fmt.Sprintf("%02d", id), "username": "user", "email": "user" + strconv.Itoa(id) + "@domain.com", "grade": id, "active": i%10 == 0, "extra": time.Now()},
// 			}
// 			result := client.ReplaceDocument("", doc)
// 			if result.Error() != nil && result.Error() != ErrNotFound {
// 				t.Fatalf("%s failed: %s", name, result.Error())
// 			} else {
// 				sessionToken = result.SessionToken
// 			}
// 		}
// 	}
// 	// if result := client.GetCollection(dbname, collname); result.Error() != nil {
// 	// 	t.Fatalf("%s failed: %s", name, result.Error())
// 	// } else {
// 	// 	fmt.Println("\tCollection etag:", result.Etag, result.Ts)
// 	// }
//
// 	req := ListDocsReq{DbName: dbname, CollName: collname, MaxItemCount: 10, ConsistencyLevel: "Session", SessionToken: sessionToken}
// 	var result *RespListDocs
// 	documents := make([]DocInfo, 0)
// 	totalRu = 0.0
// 	for result = client.ListDocuments(req); result.Error() == nil; {
// 		totalRu += result.RequestCharge
// 		documents = append(documents, result.Documents...)
// 		if result.ContinuationToken == "" {
// 			break
// 		}
// 		req.ContinuationToken = result.ContinuationToken
// 		result = client.ListDocuments(req)
// 	}
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
// 	if result.Error() != nil {
// 		t.Fatalf("%s failed: %s", name, result.Error())
// 	}
// 	if len(documents) != 100-len(removed) {
// 		fmt.Printf("Removed: %#v\n", removed)
// 		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 100-len(removed), len(documents))
// 	}
//
// 	req.DbName = dbname
// 	req.CollName = "table_not_found"
// 	if result := client.ListDocuments(req); result.CallErr != nil {
// 		t.Fatalf("%s failed: %s", name, result.CallErr)
// 	} else if result.StatusCode != 404 {
// 		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
// 	}
//
// 	client.DeleteDatabase("db_not_found")
// 	req.DbName = "db_not_found"
// 	req.CollName = collname
// 	if result := client.ListDocuments(req); result.CallErr != nil {
// 		t.Fatalf("%s failed: %s", name, result.CallErr)
// 	} else if result.StatusCode != 404 {
// 		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
// 	}
// }
//
// func TestRestClient_ListDocumentsCrossPartition(t *testing.T) {
// 	name := "TestRestClient_ListDocumentsCrossPartition"
// 	client := _newRestClient(t, name)
//
// 	dbname := "mydb"
// 	collname := "mytable"
// 	client.DeleteDatabase(dbname)
// 	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
// 	client.CreateCollection(CollectionSpec{
// 		DbName:           dbname,
// 		CollName:         collname,
// 		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
// 		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
// 	})
// 	totalRu := 0.0
// 	for i := 0; i < 100; i++ {
// 		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user" + strconv.Itoa(i%4), "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
// 		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user" + strconv.Itoa(i%4)}, DocumentData: docInfo}); result.Error() != nil {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		} else {
// 			totalRu += result.RequestCharge
// 		}
// 	}
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)
//
// 	rand.Seed(time.Now().UnixNano())
// 	removed := make(map[int]bool)
// 	for i := 0; i < 5; i++ {
// 		id := rand.Intn(100)
// 		removed[id] = true
// 		result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: fmt.Sprintf("%02d", id), PartitionKeyValues: []interface{}{"user" + strconv.Itoa(id%4)}})
// 		if result.Error() != nil && result.StatusCode != 404 {
// 			t.Fatalf("%s failed: %s", name, result.Error())
// 		}
//
// 		id = rand.Intn(100)
// 		if !removed[id] {
// 			doc := DocumentSpec{
// 				DbName:             dbname,
// 				CollName:           collname,
// 				IsUpsert:           true,
// 				PartitionKeyValues: []interface{}{"user" + strconv.Itoa(id%4)},
// 				DocumentData:       map[string]interface{}{"id": fmt.Sprintf("%02d", id), "username": "user" + strconv.Itoa(id%4), "email": "user" + strconv.Itoa(id) + "@domain.com", "grade": id, "active": i%10 == 0, "extra": time.Now()},
// 			}
// 			client.ReplaceDocument("", doc)
// 		}
// 	}
//
// 	req := ListDocsReq{DbName: dbname, CollName: collname, MaxItemCount: 10}
// 	var result *RespListDocs
// 	documents := make([]DocInfo, 0)
// 	totalRu = 0.0
// 	for result = client.ListDocuments(req); result.Error() == nil; {
// 		totalRu += result.RequestCharge
// 		documents = append(documents, result.Documents...)
// 		if result.ContinuationToken == "" {
// 			break
// 		}
// 		req.ContinuationToken = result.ContinuationToken
// 		result = client.ListDocuments(req)
// 	}
// 	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
// 	if result.Error() != nil {
// 		t.Fatalf("%s failed: %s", name, result.Error())
// 	}
// 	if len(documents) != 100-len(removed) {
// 		fmt.Printf("Removed: %#v\n", removed)
// 		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 100-len(removed), len(documents))
// 	}
//
// 	req.DbName = dbname
// 	req.CollName = "table_not_found"
// 	if result := client.ListDocuments(req); result.CallErr != nil {
// 		t.Fatalf("%s failed: %s", name, result.CallErr)
// 	} else if result.StatusCode != 404 {
// 		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
// 	}
//
// 	client.DeleteDatabase("db_not_found")
// 	req.DbName = "db_not_found"
// 	req.CollName = collname
// 	if result := client.ListDocuments(req); result.CallErr != nil {
// 		t.Fatalf("%s failed: %s", name, result.CallErr)
// 	} else if result.StatusCode != 404 {
// 		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
// 	}
// }
