package gocosmos

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

/*----------------------------------------------------------------------*/

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

/*----------------------------------------------------------------------*/

func TestRestClient_CreateDatabase(t *testing.T) {
	name := "TestRestClient_CreateDatabase"
	client := _newRestClient(t, name)

	dbname := "mydb"
	dbspecList := []DatabaseSpec{
		{Id: dbname},
		{Id: dbname, Ru: 400},
		{Id: dbname, MaxRu: 10000},
	}
	for _, dbspec := range dbspecList {
		client.DeleteDatabase(dbname)
		var dbInfo DbInfo
		if result := client.CreateDatabase(dbspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != dbname {
			t.Fatalf("%s failed: <db-id> expected %#v but received %#v", name, dbname, result.Id)
		} else if result.Rid == "" || result.Users == "" || result.Colls == "" || result.Etag == "" || result.Self == "" || result.Ts <= 0 {
			t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DbInfo)
		} else {
			dbInfo = result.DbInfo
		}

		if dbspec.Ru > 0 || dbspec.MaxRu > 0 {
			if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
				t.Fatalf("%s failed: %s", name, result.Error())
			} else {
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); dbspec.Ru > 0 && (dbspec.Ru != ru || dbspec.Ru != maxru) {
					t.Fatalf("%s failed: <offer-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, dbspec.Ru, ru, maxru)
				}
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); dbspec.MaxRu > 0 && (dbspec.MaxRu != ru*10 || dbspec.MaxRu != maxru) {
					t.Fatalf("%s failed: <max-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, dbspec.MaxRu, ru, maxru)
				}
			}
		}

		if result := client.CreateDatabase(dbspec); result.CallErr != nil {
			t.Fatalf("%s failed: %s", name, result.CallErr)
		} else if result.StatusCode != 409 {
			t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
		}
	}
}

func TestRestClient_ChangeOfferDatabase(t *testing.T) {
	name := "TestRestClient_ChangeOfferDatabase"
	client := _newRestClient(t, name)

	dbname := "mydb"
	dbspec := DatabaseSpec{Id: dbname, Ru: 400}
	client.DeleteDatabase(dbname)
	var dbInfo DbInfo
	if result := client.CreateDatabase(dbspec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		dbInfo = result.DbInfo
	}

	// database is created with manual ru=400
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); ru != 400 || maxru != 400 {
		t.Fatalf("%s failed: <ru|maxru> expected %#v|%#v but recevied %#v|%#v", name, 400, 400, ru, maxru)
	}

	// change database's manual ru to 500
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 500, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 500 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 500, auto, ru)
	}
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 500 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 500, auto, ru)
	}

	// change database's autopilot ru to 6000
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 0, 6000); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 6000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 6000, auto, maxru)
	}
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 6000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 6000, auto, maxru)
	}

	// change database's autopilot ru to 7000
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 0, 7000); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 7000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 7000, auto, maxru)
	}
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 7000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 7000, auto, maxru)
	}

	// change database's manual ru to 800
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 800, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 800 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 800, auto, ru)
	}
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 800 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 800, auto, ru)
	}

	// change database's autopilot ru to auto
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 0, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}

	// change database's autopilot ru to auto (again)
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 0, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
	if result := client.GetOfferForResource(dbInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
}

func TestRestClient_ChangeOfferDatabaseInvalid(t *testing.T) {
	name := "TestRestClient_ChangeOfferDatabaseInvalid"
	client := _newRestClient(t, name)

	client.DeleteDatabase("mydb")
	dbspec := DatabaseSpec{Id: "mydb"}
	var dbInfo DbInfo
	if result := client.CreateDatabase(dbspec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		dbInfo = result.DbInfo
	}

	if result := client.GetOfferForResource("not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}
	if result := client.ReplaceOfferForResource("not_found", 400, 0); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}

	if result := client.GetOfferForResource(dbInfo.Rid); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}
	if result := client.ReplaceOfferForResource(dbInfo.Rid, 400, 0); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}

	if result := client.ReplaceOfferForResource(dbInfo.Rid, 400, 4000); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 400 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 400, result.StatusCode)
	}
}

func TestRestClient_DeleteDatabase(t *testing.T) {
	name := "TestRestClient_DeleteDatabase"
	client := _newRestClient(t, name)

	dbname := "mydb"
	client.CreateDatabase(DatabaseSpec{Id: dbname, Ru: 400, MaxRu: 0})
	if result := client.DeleteDatabase(dbname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if result := client.DeleteDatabase(dbname); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_GetDatabase(t *testing.T) {
	name := "TestRestClient_GetDatabase"
	client := _newRestClient(t, name)

	dbname := "mydb"
	client.CreateDatabase(DatabaseSpec{Id: dbname, Ru: 400, MaxRu: 0})
	client.DeleteDatabase("db_not_found")
	if result := client.GetDatabase(dbname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.Id != dbname {
		t.Fatalf("%s failed: <db-id> expected %#v but received %#v", name, dbname, result.Id)
	} else if result.Rid == "" || result.Users == "" || result.Colls == "" || result.Etag == "" || result.Self == "" || result.Ts <= 0 {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DbInfo)
	}
	if result := client.GetDatabase("db_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ListDatabases(t *testing.T) {
	name := "TestRestClient_ListDatabases"
	client := _newRestClient(t, name)

	dbnames := map[string]int{"db1": 1, "db3": 1, "db5": 1, "db4": 1, "db2": 1}
	for dbname := range dbnames {
		client.CreateDatabase(DatabaseSpec{Id: dbname, Ru: 400, MaxRu: 0})
	}
	if result := client.ListDatabases(); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if int(result.Count) < len(dbnames) {
		t.Fatalf("%s failed: number of returned databases %#v", name, result.Count)
	} else {
		for _, db := range result.Databases {
			delete(dbnames, db.Id)
		}
		if len(dbnames) != 0 {
			t.Fatalf("%s failed: databases not returned %#v", name, dbnames)
		}
	}
}

/*----------------------------------------------------------------------*/

func TestRestClient_CreateCollection(t *testing.T) {
	name := "TestRestClient_CreateCollection"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	collspecList := []CollectionSpec{
		{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}},
		{DbName: dbname, CollName: collname, Ru: 400, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}},
		{DbName: dbname, CollName: collname, MaxRu: 10000, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}},
	}
	for _, collspec := range collspecList {
		client.DeleteDatabase(dbname)
		client.CreateDatabase(DatabaseSpec{Id: dbname})
		var collInfo CollInfo
		if result := client.CreateCollection(collspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != collname {
			t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name+"/CreateDatabase", collname, result.Id)
		} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
			result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
			result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 {
			t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
		} else {
			collInfo = result.CollInfo
		}

		if collspec.Ru > 0 || collspec.MaxRu > 0 {
			if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
				t.Fatalf("%s failed: %s", name, result.Error())
			} else {
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); collspec.Ru > 0 && (collspec.Ru != ru || collspec.Ru != maxru) {
					t.Fatalf("%s failed: <offer-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, collspec.Ru, ru, maxru)
				}
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); collspec.MaxRu > 0 && (collspec.MaxRu != ru*10 || collspec.MaxRu != maxru) {
					t.Fatalf("%s failed: <max-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, collspec.MaxRu, ru, maxru)
				}
			}
		}

		if result := client.CreateCollection(collspec); result.CallErr != nil {
			t.Fatalf("%s failed: %s", name, result.CallErr)
		} else if result.StatusCode != 409 {
			t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
		}
	}

	client.DeleteDatabase("db_not_found")
	if result := client.CreateCollection(CollectionSpec{
		DbName:           "db_not_found",
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ChangeOfferCollection(t *testing.T) {
	name := "TestRestClient_ChangeOfferCollection"
	client := _newRestClient(t, name)

	dbname := "mydb"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	collname := "mytable"
	collspec := CollectionSpec{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}}

	var collInfo CollInfo
	if result := client.CreateCollection(collspec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		collInfo = result.CollInfo
	}

	// collection is created with manual ru=400
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); ru != 400 || maxru != 400 {
		t.Fatalf("%s failed: <ru|maxru> expected %#v|%#v but recevied %#v|%#v", name, 400, 400, ru, maxru)
	}

	// change collection's manual ru to 500
	if result := client.ReplaceOfferForResource(collInfo.Rid, 500, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 500 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 500, auto, ru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 500 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 500, auto, ru)
	}

	// change collection's autopilot ru to 6000
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 6000); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 6000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 6000, auto, maxru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 6000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 6000, auto, maxru)
	}

	// change collection's autopilot ru to 7000
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 7000); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 7000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 7000, auto, maxru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 7000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 7000, auto, maxru)
	}

	// change collection's manual ru to 800
	if result := client.ReplaceOfferForResource(collInfo.Rid, 800, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 800 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 800, auto, ru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 800 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 800, auto, ru)
	}

	// change collection's autopilot ru to auto
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}

	// change collection's autopilot ru to auto (again)
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
}

func TestRestClient_ChangeOfferCollectionInvalid(t *testing.T) {
	name := "TestRestClient_ChangeOfferCollectionInvalid"
	client := _newRestClient(t, name)

	dbname := "mydb"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	collname := "mytable"
	collspec := CollectionSpec{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}}

	var collInfo CollInfo
	if result := client.CreateCollection(collspec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		collInfo = result.CollInfo
	}

	if result := client.GetOfferForResource("not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}
	if result := client.ReplaceOfferForResource("not_found", 400, 0); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}

	if result := client.ReplaceOfferForResource(collInfo.Rid, 400, 4000); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 400 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 400, result.StatusCode)
	}
}

func TestRestClient_CreateCollectionIndexingPolicy(t *testing.T) {
	name := "TestRestClient_CreateCollectionIndexingPolicy"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	collSpec := CollectionSpec{
		DbName: dbname, CollName: collname,
		IndexingPolicy:   map[string]interface{}{"indexingMode": "consistent", "automatic": true},
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}}
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	if result := client.CreateCollection(collSpec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
}

func TestRestClient_ReplaceCollection(t *testing.T) {
	name := "TestRestClient_ReplaceCollection"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}})

	collspecList := []CollectionSpec{
		{DbName: dbname, CollName: collname, Ru: 800, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
			IndexingPolicy: map[string]interface{}{"indexingMode": "consistent", "automatic": true,
				"includedPaths": []map[string]interface{}{{"path": "/*", "indexes": []map[string]interface{}{{"dataType": "Number", "precision": -1, "kind": "Range"}}}}, "excludedPaths": []map[string]interface{}{},
			}},
		{DbName: dbname, CollName: collname, MaxRu: 8000, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
			IndexingPolicy: map[string]interface{}{"indexingMode": "consistent", "automatic": true,
				"includedPaths": []map[string]interface{}{{"path": "/*", "indexes": []map[string]interface{}{{"dataType": "String", "precision": 3, "kind": "Hash"}}}}, "excludedPaths": []map[string]interface{}{},
			}},
	}
	for _, colspec := range collspecList {
		if result := client.ReplaceCollection(colspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != collname {
			t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name+"/CreateDatabase", collname, result.Id)
		} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
			result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
			result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 {
			t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
		}
	}

	if result := client.ReplaceCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         "table_not_found",
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	if result := client.ReplaceCollection(CollectionSpec{
		DbName:           "db_not_found",
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_DeleteCollection(t *testing.T) {
	name := "TestRestClient_DeleteCollection"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mycoll"
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	})
	if result := client.DeleteCollection(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if result := client.DeleteCollection(dbname, collname); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	if result := client.DeleteCollection("db_not_found", collname); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_GetCollection(t *testing.T) {
	name := "TestRestClient_GetCollection"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	})
	if result := client.GetCollection(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.Id != collname {
		t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name, collname, result.Id)
	} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
		result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
		result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 ||
		len(result.ConflictResolutionPolicy) == 0 || len(result.GeospatialConfig) == 0 {
		t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
	}

	if result := client.GetCollection(dbname, "table_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	if result := client.GetCollection("db_not_found", "table_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ListCollection(t *testing.T) {
	name := "TestRestClient_ListCollection"
	client := _newRestClient(t, name)

	dbname := "mydb"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	collnames := map[string]int{"table1": 1, "table3": 1, "table5": 1, "table4": 1, "table2": 1}
	for collname := range collnames {
		client.CreateCollection(CollectionSpec{
			DbName:           dbname,
			CollName:         collname,
			PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		})
	}
	if result := client.ListCollections(dbname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if int(result.Count) != len(collnames) {
		t.Fatalf("%s failed: number of returned collections %#v", name, result.Count)
	} else {
		for _, coll := range result.Collections {
			delete(collnames, coll.Id)
		}
		if len(collnames) != 0 {
			t.Fatalf("%s failed: collections not returned %#v", name, collnames)
		}
	}

	client.DeleteDatabase("db_not_found")
	if result := client.ListCollections("db_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_GetPkranges(t *testing.T) {
	name := "TestRestClient_GetPkranges"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{DbName: dbname, CollName: collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
	})
	var wait sync.WaitGroup
	n := 1000
	d := 16
	wait.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			id := fmt.Sprintf("%04d", i)
			username := "user" + fmt.Sprintf("%02x", i%d)
			client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname,
				PartitionKeyValues: []interface{}{username},
				DocumentData:       map[string]interface{}{"id": id, "username": username, "index": i},
			})
			wait.Done()
		}(i)
	}
	wait.Wait()

	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if len(result.Pkranges) < 1 {
		t.Fatalf("%s failed: invalid number of pk ranges %#v", name, len(result.Pkranges))
	}

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

/*----------------------------------------------------------------------*/

func TestRestClient_CreateDocument(t *testing.T) {
	name := "TestRestClient_CreateDocument"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "1" || result.DocInfo["username"] != "user" || result.DocInfo["email"] != "user@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, IndexingDirective: "Include",
		DocumentData: map[string]interface{}{"id": "11", "username": "user", "email": "user11@domain.com", "grade": 1.1, "active": false},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "11" || result.DocInfo["username"] != "user" || result.DocInfo["email"] != "user11@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.1 || result.DocInfo["active"] != false || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, IndexingDirective: "Exclude",
		DocumentData: map[string]interface{}{"id": "111", "username": "user", "email": "user111@domain.com", "grade": 1.11, "active": false},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "111" || result.DocInfo["username"] != "user" || result.DocInfo["email"] != "user111@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.11 || result.DocInfo["active"] != false || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// duplicated id
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain1.com", "grade": 2, "active": false},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	// duplicated unique index
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "2", "username": "user", "email": "user@domain.com", "grade": 3, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	// collection not found
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: "table_not_found", PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	// database not found
	client.DeleteDatabase("db_not_found")
	if result := client.CreateDocument(DocumentSpec{
		DbName: "db_not_found", CollName: collname, PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_CreateDocumentNoId(t *testing.T) {
	name := "TestRestClient_CreateDocumentNoId"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
	})

	client.autoId = true
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"},
		DocumentData: map[string]interface{}{"username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] == "" || result.DocInfo["username"] != "user1" || result.DocInfo["email"] != "user1@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	client.autoId = false
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user2"},
		DocumentData: map[string]interface{}{"username": "user2", "email": "user2@domain.com", "grade": 2, "active": false},
	}); result.Error() == nil {
		t.Fatalf("%s failed: this operation should not be successful", name)
	}
}

func TestRestClient_UpsertDocument(t *testing.T) {
	name := "TestRestClient_UpsertDocument"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "1" || result.DocInfo["username"] != "user1" || result.DocInfo["email"] != "user1@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user2"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "2", "username": "user2", "email": "user2@domain.com", "grade": 2, "active": false},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "2" || result.DocInfo["username"] != "user2" || result.DocInfo["email"] != "user2@domain.com" ||
		result.DocInfo["grade"].(float64) != 2.0 || result.DocInfo["active"] != false || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "username": "user1", "email": "user1@domain1.com", "grade": 2, "active": false, "data": "value"},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "1" || result.DocInfo["username"] != "user1" || result.DocInfo["email"] != "user1@domain1.com" ||
		result.DocInfo["grade"].(float64) != 2.0 || result.DocInfo["active"] != false || result.DocInfo["data"] != "value" || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// duplicated unique key
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "3", "username": "user1", "email": "user1@domain1.com", "grade": 2, "active": false, "data": "value"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	// collection not found
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: "table_not_found", PartitionKeyValues: []interface{}{"user"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	// database not found
	client.DeleteDatabase("db_not_found")
	if result := client.CreateDocument(DocumentSpec{
		DbName: "db_not_found", CollName: collname, PartitionKeyValues: []interface{}{"user"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_UpsertDocumentNoId(t *testing.T) {
	name := "TestRestClient_UpsertDocumentNoId"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	client.autoId = true
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] == "" || result.DocInfo["username"] != "user1" || result.DocInfo["email"] != "user1@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	client.autoId = false
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user2"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"username": "user2", "email": "user2@domain.com", "grade": 2, "active": false},
	}); result.Error() == nil {
		t.Fatalf("%s failed: this operation should not be successful", name)
	}
}

func TestRestClient_ReplaceDocument(t *testing.T) {
	name := "TestRestClient_ReplaceDocument"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	// insert 2 documents
	docInfo := map[string]interface{}{"id": "2", "username": "user", "email": "user2@domain.com", "grade": 2.0, "active": false}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}
	docInfo = map[string]interface{}{"id": "1", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// conflict unique key
	docInfo["email"] = "user2@domain.com"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	// replace document without etag matching
	var etag string
	docInfo = map[string]interface{}{"id": "1", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	docInfo["grade"] = 2.0
	docInfo["active"] = false
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	} else {
		etag = result.DocInfo["_etag"].(string)
	}

	// replace document with etag matching: should not match
	docInfo["email"] = "user3@domain.com"
	docInfo["grade"] = 3.0
	docInfo["active"] = true
	if result := client.ReplaceDocument(etag+"dummy", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 412 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 412, result.StatusCode)
	}
	// replace document with etag matching: should match
	if result := client.ReplaceDocument(etag, DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// document not found
	docInfo["id"] = "0"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	// collection not found
	docInfo["id"] = "1"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: "tbl_not_found", PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	// database not found
	client.DeleteDatabase("db_not_found")
	if result := client.ReplaceDocument("", DocumentSpec{DbName: "db_not_found", CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ReplaceDocumentCrossPartition(t *testing.T) {
	name := "TestRestClient_ReplaceDocumentCrossPartition"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	// insert a document
	docInfo := map[string]interface{}{"id": "1", "username": "user1", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	docInfo["username"] = "user2"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user2"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	docInfo["username"] = "user2"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 400 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 400, result.StatusCode)
	}
}

func TestRestClient_GetDocument(t *testing.T) {
	name := "TestRestClient_GetDocument"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	var etag, sessionToken string
	docInfo := map[string]interface{}{"id": "1", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		etag = result.DocInfo["_etag"].(string)
		sessionToken = result.SessionToken
	}

	if result := client.GetDocument(DocReq{DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo.Id() != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo.Rid() == "" ||
		result.DocInfo.Self() == "" || result.DocInfo.Ts() == 0 || result.DocInfo.Etag() == "" || result.DocInfo.Attachments() == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	} else {
		ago := time.Now().Add(-5 * time.Minute)
		docTime := result.DocInfo.TsAsTime()
		if !ago.Before(docTime) {
			t.Fatalf("%s failed: invalid document time %s", name, docTime)
		}

		clone := result.DocInfo.RemoveSystemAttrs()
		for k := range clone {
			if strings.HasPrefix(k, "_") {
				t.Fatalf("%s failed: invalid cloned document %#v", name, clone)
			}
		}
	}

	if result := client.GetDocument(DocReq{NotMatchEtag: etag + "dummy", DbName: dbname, CollName: collname, DocId: "1",
		ConsistencyLevel: "Session", SessionToken: sessionToken,
		PartitionKeyValues: []interface{}{"user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["username"] != docInfo["username"] || result.DocInfo["email"] != docInfo["email"] ||
		result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	if result := client.GetDocument(DocReq{NotMatchEtag: etag, DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 304 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 304, result.StatusCode)
	}

	if result := client.GetDocument(DocReq{DbName: dbname, CollName: collname, DocId: "0", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	if result := client.GetDocument(DocReq{DbName: dbname, CollName: "tbl_not_found", DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	if result := client.GetDocument(DocReq{DbName: "db_not_found", CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_DeleteDocument(t *testing.T) {
	name := "TestRestClient_DeleteDocument"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	var etag string
	docInfo := map[string]interface{}{"id": "1", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		etag = result.DocInfo["_etag"].(string)
	}

	if result := client.DeleteDocument(DocReq{MatchEtag: etag + "dummy", DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 412 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
	if result := client.DeleteDocument(DocReq{MatchEtag: etag, DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}

	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}

	if result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	if result := client.DeleteDocument(DocReq{DbName: dbname, CollName: "tbl_not_found", DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	if result := client.DeleteDocument(DocReq{DbName: "db_not_found", CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_QueryDocuments(t *testing.T) {
	name := "TestRestClient_QueryDocuments"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})
	totalRu := 0.0
	var sessionToken string
	for i := 0; i < 100; i++ {
		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user", "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else {
			totalRu += result.RequestCharge
			sessionToken = result.SessionToken
		}
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)

	query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, ConsistencyLevel: "Session", SessionToken: sessionToken,
		Query:  "SELECT * FROM c WHERE c.id>=@id AND c.username='user'",
		Params: []interface{}{map[string]interface{}{"name": "@id", "value": "37"}},
	}
	var result *RespQueryDocs
	documents := make([]DocInfo, 0)
	totalRu = 0.0
	for result = client.QueryDocuments(query); result.Error() == nil; {
		totalRu += result.RequestCharge
		documents = append(documents, result.Documents...)
		if result.ContinuationToken == "" {
			break
		}
		query.ContinuationToken = result.ContinuationToken
		result = client.QueryDocuments(query)
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
	if result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if len(documents) != 63 {
		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 63, len(documents))
	}

	query.DbName = dbname
	query.CollName = "table_not_found"
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	query.DbName = "db_not_found"
	query.CollName = collname
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_QueryDocumentsCrossPartition(t *testing.T) {
	name := "TestRestClient_QueryDocumentsCrossPartition"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})
	totalRu := 0.0
	for i := 0; i < 100; i++ {
		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user" + strconv.Itoa(i%4), "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user" + strconv.Itoa(i%4)}, DocumentData: docInfo}); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else {
			totalRu += result.RequestCharge
		}
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)

	query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, CrossPartitionEnabled: true,
		Query:  "SELECT * FROM c WHERE c.id>=@id",
		Params: []interface{}{map[string]interface{}{"name": "@id", "value": "37"}},
	}
	var result *RespQueryDocs
	documents := make([]DocInfo, 0)
	totalRu = 0.0
	for result = client.QueryDocuments(query); result.Error() == nil; {
		totalRu += result.RequestCharge
		documents = append(documents, result.Documents...)
		if result.ContinuationToken == "" {
			break
		}
		query.ContinuationToken = result.ContinuationToken
		result = client.QueryDocuments(query)
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
	if result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if len(documents) != 63 {
		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 63, len(documents))
	}

	query.DbName = dbname
	query.CollName = "table_not_found"
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	query.DbName = "db_not_found"
	query.CollName = collname
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_QueryDocumentsPkranges(t *testing.T) {
	name := "TestRestClient_QueryDocumentsPkranges"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})
	totalRu := 0.0
	var wait sync.WaitGroup
	n := 100
	d := 16
	wait.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			id := fmt.Sprintf("%04d", i)
			username := "user" + fmt.Sprintf("%02x", i%d)
			email := "user" + strconv.Itoa(i) + "@domain.com"
			if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname,
				PartitionKeyValues: []interface{}{username},
				DocumentData:       map[string]interface{}{"id": id, "username": username, "email": email, "index": i},
			}); result.Error() != nil {
				t.Fatalf("%s failed: %s", name, result.Error())
			} else {
				totalRu += result.RequestCharge
			}
			wait.Done()
		}(i)
	}
	wait.Wait()
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)

	{
		query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, CrossPartitionEnabled: true,
			Query:  "SELECT * FROM c WHERE c.id>=@id ORDER BY c.id OFFSET 5 LIMIT 3",
			Params: []interface{}{map[string]interface{}{"name": "@id", "value": "0037"}},
		}
		var result *RespQueryDocs
		documents := make([]DocInfo, 0)
		totalRu = 0.0
		for result = client.QueryDocuments(query); result.Error() == nil; {
			totalRu += result.RequestCharge
			documents = append(documents, result.Documents...)
			if result.ContinuationToken == "" {
				break
			}
			query.ContinuationToken = result.ContinuationToken
			result = client.QueryDocuments(query)
		}
		fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
		if result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		}
		if len(documents) != 3 {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 3, len(documents))
		}
		if documents[0].Id() != "0042" || documents[1].Id() != "0043" || documents[2].Id() != "0044" {
			t.Fatalf("%s failed: <documents> not in correct order", name)
		}
	}

	{
		query := QueryReq{DbName: dbname, CollName: collname, MaxItemCount: 10, CrossPartitionEnabled: true,
			Query:  "SELECT c.username, sum(c.index) FROM c WHERE c.id<@id GROUP BY c.username",
			Params: []interface{}{map[string]interface{}{"name": "@id", "value": "0123"}},
		}
		var result *RespQueryDocs
		documents := make([]DocInfo, 0)
		totalRu = 0.0
		for result = client.QueryDocuments(query); result.Error() == nil; {
			totalRu += result.RequestCharge
			documents = append(documents, result.Documents...)
			if result.ContinuationToken == "" {
				break
			}
			query.ContinuationToken = result.ContinuationToken
			result = client.QueryDocuments(query)
		}
		fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
		if result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		}
		if len(documents) != d {
			t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, d, len(documents))
		}
	}
}

func TestRestClient_ListDocuments(t *testing.T) {
	name := "TestRestClient_ListDocuments"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})
	totalRu := 0.0

	// if result := client.GetCollection(dbname, collname); result.Error() != nil {
	// 	t.Fatalf("%s failed: %s", name, result.Error())
	// } else {
	// 	fmt.Println("\tCollection etag:", result.Etag, result.Ts)
	// }

	for i := 0; i < 100; i++ {
		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user", "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"}, DocumentData: docInfo}); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else {
			totalRu += result.RequestCharge
		}
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)

	// var collEtag string
	// if result := client.GetCollection(dbname, collname); result.Error() != nil {
	// 	t.Fatalf("%s failed: %s", name, result.Error())
	// } else {
	// 	collEtag = result.Etag
	// 	fmt.Println("\tCollection etag:", result.Etag, result.Ts)
	// }

	var sessionToken string
	rand.Seed(time.Now().UnixNano())
	removed := make(map[int]bool)
	for i := 0; i < 5; i++ {
		id := rand.Intn(100)
		removed[id] = true
		result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: fmt.Sprintf("%02d", id), PartitionKeyValues: []interface{}{"user"}})
		if result.Error() != nil && result.StatusCode != 404 {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else {
			sessionToken = result.SessionToken
		}

		id = rand.Intn(100)
		if !removed[id] {
			doc := DocumentSpec{
				DbName:             dbname,
				CollName:           collname,
				IsUpsert:           true,
				PartitionKeyValues: []interface{}{"user"},
				DocumentData:       map[string]interface{}{"id": fmt.Sprintf("%02d", id), "username": "user", "email": "user" + strconv.Itoa(id) + "@domain.com", "grade": id, "active": i%10 == 0, "extra": time.Now()},
			}
			result := client.ReplaceDocument("", doc)
			if result.Error() != nil && result.Error() != ErrNotFound {
				t.Fatalf("%s failed: %s", name, result.Error())
			} else {
				sessionToken = result.SessionToken
			}
		}
	}
	// if result := client.GetCollection(dbname, collname); result.Error() != nil {
	// 	t.Fatalf("%s failed: %s", name, result.Error())
	// } else {
	// 	fmt.Println("\tCollection etag:", result.Etag, result.Ts)
	// }

	req := ListDocsReq{DbName: dbname, CollName: collname, MaxItemCount: 10, ConsistencyLevel: "Session", SessionToken: sessionToken}
	var result *RespListDocs
	documents := make([]DocInfo, 0)
	totalRu = 0.0
	for result = client.ListDocuments(req); result.Error() == nil; {
		totalRu += result.RequestCharge
		documents = append(documents, result.Documents...)
		if result.ContinuationToken == "" {
			break
		}
		req.ContinuationToken = result.ContinuationToken
		result = client.ListDocuments(req)
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
	if result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if len(documents) != 100-len(removed) {
		fmt.Printf("Removed: %#v\n", removed)
		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 100-len(removed), len(documents))
	}

	req.DbName = dbname
	req.CollName = "table_not_found"
	if result := client.ListDocuments(req); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	req.DbName = "db_not_found"
	req.CollName = collname
	if result := client.ListDocuments(req); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ListDocumentsCrossPartition(t *testing.T) {
	name := "TestRestClient_ListDocumentsCrossPartition"
	client := _newRestClient(t, name)

	dbname := "mydb"
	collname := "mytable"
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname, MaxRu: 10000})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})
	totalRu := 0.0
	for i := 0; i < 100; i++ {
		docInfo := map[string]interface{}{"id": fmt.Sprintf("%02d", i), "username": "user" + strconv.Itoa(i%4), "email": "user" + strconv.Itoa(i) + "@domain.com", "grade": i, "active": i%10 == 0}
		if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user" + strconv.Itoa(i%4)}, DocumentData: docInfo}); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else {
			totalRu += result.RequestCharge
		}
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Insert", totalRu)

	rand.Seed(time.Now().UnixNano())
	removed := make(map[int]bool)
	for i := 0; i < 5; i++ {
		id := rand.Intn(100)
		removed[id] = true
		result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: fmt.Sprintf("%02d", id), PartitionKeyValues: []interface{}{"user" + strconv.Itoa(id%4)}})
		if result.Error() != nil && result.StatusCode != 404 {
			t.Fatalf("%s failed: %s", name, result.Error())
		}

		id = rand.Intn(100)
		if !removed[id] {
			doc := DocumentSpec{
				DbName:             dbname,
				CollName:           collname,
				IsUpsert:           true,
				PartitionKeyValues: []interface{}{"user" + strconv.Itoa(id%4)},
				DocumentData:       map[string]interface{}{"id": fmt.Sprintf("%02d", id), "username": "user" + strconv.Itoa(id%4), "email": "user" + strconv.Itoa(id) + "@domain.com", "grade": id, "active": i%10 == 0, "extra": time.Now()},
			}
			client.ReplaceDocument("", doc)
		}
	}

	req := ListDocsReq{DbName: dbname, CollName: collname, MaxItemCount: 10}
	var result *RespListDocs
	documents := make([]DocInfo, 0)
	totalRu = 0.0
	for result = client.ListDocuments(req); result.Error() == nil; {
		totalRu += result.RequestCharge
		documents = append(documents, result.Documents...)
		if result.ContinuationToken == "" {
			break
		}
		req.ContinuationToken = result.ContinuationToken
		result = client.ListDocuments(req)
	}
	fmt.Printf("\t%s - total RU charged: %0.3f\n", name+"/Query", totalRu)
	if result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if len(documents) != 100-len(removed) {
		fmt.Printf("Removed: %#v\n", removed)
		t.Fatalf("%s failed: <num-document> expected %#v but received %#v", name, 100-len(removed), len(documents))
	}

	req.DbName = dbname
	req.CollName = "table_not_found"
	if result := client.ListDocuments(req); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	client.DeleteDatabase("db_not_found")
	req.DbName = "db_not_found"
	req.CollName = collname
	if result := client.ListDocuments(req); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}
