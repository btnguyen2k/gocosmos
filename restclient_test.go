package gocosmos

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

const (
	testDb    = "mydb"
	testTable = "mytable"
)

func TestNewRestClient(t *testing.T) {
	name := "TestNewRestClient"
	if _, err := NewRestClient(nil, "dummy"); err == nil {
		t.Fatalf("%s failed: connection string should be invalid", name)
	}
	if _, err := NewRestClient(nil, "AccountEndpoint=;AccountKey=dummy"); err == nil {
		t.Fatalf("%s failed: connection string should be invalid", name)
	}
	if _, err := NewRestClient(nil, "AccountEndpoint=dummy;AccountKey="); err == nil {
		t.Fatalf("%s failed: connection string should be invalid", name)
	}
	if _, err := NewRestClient(nil, "AccountEndpoint=dummy;AccountKey=dummy"); err == nil {
		t.Fatalf("%s failed: connection string should be invalid", name)
	}
	accountKey := "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	if _, err := NewRestClient(nil, "AccountEndpoint=dummy;AccountKey="+accountKey); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if client, err := NewRestClient(nil, "AccountEndpoint=dummy;AccountKey="+accountKey+";Version=1.2.3;TimeoutMs=12345"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v := client.apiVersion; v != "1.2.3" {
		t.Fatalf("%s failed: expected API version to be %#v but received %#v", name, "1.2.3", v)
	}
}

func _newRestClient(t *testing.T, testName string) *RestClient {
	cosmosUrl := strings.TrimSpace(strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, ""))
	if cosmosUrl == "" {
		t.Skipf("%s skipped", testName)
	}
	client, err := NewRestClient(nil, cosmosUrl)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/NewRestClient", err)
	}
	return client
}

func TestRestClient_GetPkranges(t *testing.T) {
	name := "TestRestClient_GetPkranges"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{DbName: dbname, CollName: collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
	})

	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if len(result.Pkranges) < 1 {
		t.Fatalf("%s failed: invalid number of pk ranges %#v", name, len(result.Pkranges))
	}

	client.DeleteCollection(dbname, "table_not_found")
	if result := client.GetPkranges(dbname, "table_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	if result := client.GetPkranges("db_not_found", collname); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func _testRestClientQueryPlan(t *testing.T, testName string, client *RestClient, dbname, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", orderField: "grade", orderDirection: "asc", rewrittenSql: true},
		{name: "OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", orderField: "grade", orderDirection: "desc", rewrittenSql: true},
		{name: "GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByField: "count", rewrittenSql: true},
		{name: "GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByField: "sum", rewrittenSql: true},
		{name: "GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByField: "min", rewrittenSql: true},
		{name: "GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByField: "max", rewrittenSql: true},
		{name: "GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByField: "average", rewrittenSql: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := QueryReq{DbName: dbname, CollName: collname, Query: testCase.query,
				Params: []interface{}{
					map[string]interface{}{"name": "@low", "value": lowStr},
					map[string]interface{}{"name": "@high", "value": highStr},
				},
			}
			result := client.QueryPlan(query)
			if result.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/QueryPlan", result.Error())
			}
			if testCase.rewrittenSql && result.QueryInfo.RewrittenQuery == "" {
				t.Fatalf("%s failed: expecting <rewritten-query> but received empty", testName+"/"+testCase.name+"/QueryPlan")
			}
			if !testCase.rewrittenSql && result.QueryInfo.RewrittenQuery != "" {
				t.Fatalf("%s failed: expecting <rewritten-query> to be nil but received %#v", testName+"/"+testCase.name+"/QueryPlan", result.QueryInfo.RewrittenQuery)
			}
			if testCase.orderField != "" && (len(result.QueryInfo.OrderBy) < 1 || len(result.QueryInfo.OrderByExpressions) < 1) {
				t.Fatalf("%s failed: expecting <order-by/expressions> but received empty", testName+"/"+testCase.name+"/QueryPlan")
			}
			if testCase.orderField == "" && (len(result.QueryInfo.OrderBy) > 0 || len(result.QueryInfo.OrderByExpressions) > 0) {
				t.Fatalf("%s failed: expecting <order-by/expressions> to be empty but received {%#v / %#v}", testName+"/"+testCase.name+"/QueryPlan", result.QueryInfo.OrderBy, result.QueryInfo.OrderByExpressions)
			}
			if testCase.groupByField != "" && (len(result.QueryInfo.GroupByExpressions) < 1 || len(result.QueryInfo.GroupByAliases) < 1 || len(result.QueryInfo.GroupByAliasToAggregateType) < 1) {
				t.Fatalf("%s failed: expecting <group-by-expressions/alias> but received empty", testName+"/"+testCase.name+"/QueryPlan")
			}
			if testCase.groupByField == "" && (len(result.QueryInfo.GroupByExpressions) > 0 || len(result.QueryInfo.GroupByAliases) > 0 || len(result.QueryInfo.GroupByAliasToAggregateType) > 0) {
				t.Fatalf("%s failed: expecting <group-by-expressions/alias> to be empty but received {%#v / %#v}", testName+"/"+testCase.name+"/QueryPlan", result.QueryInfo.OrderBy, result.QueryInfo.OrderByExpressions)
			}
		})
	}
}

func TestRestClient_QueryPlan_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryPlan_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryPlan(t, testName, client, dbname, collname)
}

func TestRestClient_QueryPlan_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryPlan_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryPlan(t, testName, client, dbname, collname)
}
