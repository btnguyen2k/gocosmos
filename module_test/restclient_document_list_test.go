package gocosmos_test

import (
	"fmt"
	"github.com/btnguyen2k/gocosmos"
	"testing"
	"time"
)

func _verifyChangeFeed(f funcTestFatal, testName string, result *gocosmos.RespListDocs, maxItemCounts, expectedNumDocs int) {
	if result.Error() != nil {
		f(fmt.Sprintf("%s failed: %s", testName, result.Error()))
	}
	if maxItemCounts > 0 && (len(result.Documents) > maxItemCounts || result.Count > maxItemCounts) {
		f(fmt.Sprintf("%s failed: <num-feed> expected not larger than %#v but received (len: %#v / count: %#v)", testName, maxItemCounts, len(result.Documents), result.Count))
	}
	if expectedNumDocs > 0 && (len(result.Documents) != expectedNumDocs || result.Count != expectedNumDocs) {
		f(fmt.Sprintf("%s failed: <num-feed> expected to be %#v but received (len: %#v / count: %#v)", testName, expectedNumDocs, len(result.Documents), result.Count))
	}
	var prevDoc gocosmos.DocInfo
	for _, doc := range result.Documents {
		if prevDoc != nil && prevDoc.Ts() > doc.Ts() {
			f(fmt.Sprintf("%s failed: out of order {id: %#v, ts: %#v} -> {id: %#v, ts: %#v}", testName, prevDoc.Id(), prevDoc.Ts(), doc.Id(), doc.Ts()))
		}
		prevDoc = doc
	}
}

func _fetchChangeFeedAndVerify(f funcTestFatal, testName string, client *gocosmos.RestClient, req gocosmos.ListDocsReq, expectedNumDocs int) *gocosmos.RespListDocs {
	var result *gocosmos.RespListDocs
	for {
		tempResult := client.ListDocuments(req)
		_verifyChangeFeed(f, testName, tempResult, req.MaxItemCount, -1)
		if result == nil {
			result = tempResult
		} else if tempResult.Count > 0 {
			result.Etag = tempResult.Etag
			result.ContinuationToken = tempResult.ContinuationToken
			result.SessionToken = tempResult.SessionToken
			result.Count += tempResult.Count
			result.Documents = append(result.Documents, tempResult.Documents...)
		}
		if tempResult.ContinuationToken == "" && tempResult.Etag == "" {
			break
		}
		if tempResult.ContinuationToken != "" {
			req.ConsistencyLevel = tempResult.ContinuationToken
		} else if tempResult.Etag != "" {
			req.NotMatchEtag = tempResult.Etag
		}
	}
	_verifyChangeFeed(f, testName, result, -1, expectedNumDocs)
	return result
}

func _testRestClientGetChangeFeed(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	testCases := []struct {
		name            string
		maxItemCount    int
		expectedNumFeed int
	}{
		{name: "AllFeed", expectedNumFeed: len(dataList)},
		{name: "AllFeed_Limit", maxItemCount: 11, expectedNumFeed: len(dataList)},
	}

	req := gocosmos.ListDocsReq{DbName: dbname, CollName: collname, IsIncrementalFeed: true, MaxItemCount: -1}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// touch
			now := time.Now()
			for _, doc := range dataList {
				username := doc.GetAttrAsTypeUnsafe("username", nil)
				doc["touch"] = now
				resp := client.ReplaceDocument(doc.Etag(), gocosmos.DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{username}, DocumentData: doc})
				if resp.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/ReplaceDocument", resp.Error())
				}
			}

			// fetch change feed
			if testCase.maxItemCount > 0 {
				req.MaxItemCount = testCase.maxItemCount
			}
			result := _fetchChangeFeedAndVerify(func(msg string) { t.Fatalf(msg) }, testName+"/"+testCase.name, client, req, testCase.expectedNumFeed)

			// generate change feed
			time.Sleep(2 * time.Second)
			now = time.Now()
			expected := 0
			for i := len(dataList) - 1; i >= 0; i-- {
				if i%3 != 0 {
					continue
				}
				expected++
				username := dataList[i].GetAttrAsTypeUnsafe("username", nil)
				dataList[i]["touch"] = now
				resp := client.ReplaceDocument(dataList[i].Etag(), gocosmos.DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{username}, DocumentData: dataList[i]})
				if resp.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/ReplaceDocument", resp.Error())
				}
			}

			// fetch change feed
			req.NotMatchEtag = result.Etag
			req.ContinuationToken = ""
			result = _fetchChangeFeedAndVerify(func(msg string) { t.Fatalf(msg) }, testName+"/"+testCase.name, client, req, expected)

			// fetch change feed, again
			req.NotMatchEtag = result.Etag
			result = client.ListDocuments(req)
			if result.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, result.Error())
			}
			if len(result.Documents) != 0 || result.Count != 0 {
				t.Fatalf("%s failed: <num-feed> expected not larger than %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name, 0, len(result.Documents), result.Count)
			}
		})
	}
}

func TestRestClient_GetChangeFeed_SmallRU(t *testing.T) {
	testName := "TestRestClient_GetChangeFeed_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientGetChangeFeed(t, testName, client, dbname, collname)
}

// func TestRestClient_GetChangeFeed_LargeRU(t *testing.T) {
// 	testName := "TestRestClient_GetChangeFeed_LargeRU"
// 	client := _newRestClient(t, testName)
// 	dbname := testDb
// 	collname := testTable
// 	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
// 	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
// 		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
// 	} else if result.Count < 2 {
// 		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
// 	}
// 	_testRestClientGetChangeFeed(t, testName, client, dbname, collname)
// }

func _testRestClientListDocuments(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	testCases := []struct {
		name            string
		maxItemCount    int
		expectedNumFeed int
	}{
		{name: "Nolimit", expectedNumFeed: len(dataList)},
		{name: "Limit", maxItemCount: 11, expectedNumFeed: 11},
	}

	req := gocosmos.ListDocsReq{DbName: dbname, CollName: collname, MaxItemCount: -1}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.maxItemCount > 0 {
				req.MaxItemCount = testCase.maxItemCount
			}
			result := client.ListDocuments(req)
			if result.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, result.Error())
			}
			expected := testCase.expectedNumFeed
			if len(result.Documents) != expected || result.Count != expected {
				t.Fatalf("%s failed: <num-feed> expected to be %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name, expected, len(result.Documents), result.Count)
			}
		})
	}
}

func TestRestClient_ListDocuments_SmallRU(t *testing.T) {
	testName := "TestRestClient_ListDocuments_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientListDocuments(t, testName, client, dbname, collname)
}

func TestRestClient_ListDocuments_LargeRU(t *testing.T) {
	testName := "TestRestClient_ListDocuments_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientListDocuments(t, testName, client, dbname, collname)
}
