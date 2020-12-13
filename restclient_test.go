package go_cosmos

import (
	"os"
	"strings"
	"testing"
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
		if result := client.CreateDatabase(dbspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != dbname {
			t.Fatalf("%s failed: <db-id> expected %#v but received %#v", name, dbname, result.Id)
		} else if result.Rid == "" || result.Users == "" || result.Colls == "" || result.Etag == "" || result.Self == "" || result.Ts <= 0 {
			t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DbInfo)
		}
		if result := client.CreateDatabase(dbspec); result.CallErr != nil {
			t.Fatalf("%s failed: %s", name, result.CallErr)
		} else if result.StatusCode != 409 {
			t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
		}
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
	for dbname, _ := range dbnames {
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
	for _, colspec := range collspecList {
		client.DeleteDatabase(dbname)
		client.CreateDatabase(DatabaseSpec{Id: dbname})
		if result := client.CreateCollection(colspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != collname {
			t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name+"/CreateDatabase", collname, result.Id)
		} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
			result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
			result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 {
			t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
		}

		if result := client.CreateCollection(colspec); result.CallErr != nil {
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
	for collname, _ := range collnames {
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
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain1.com", "grade": 2, "active": false},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		// duplicated id
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "2", "username": "user", "email": "user@domain.com", "grade": 3, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		// duplicated unique index
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: "table_not_found", PartitionKeyValues: []interface{}{"user"},
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

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

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "3", "username": "user1", "email": "user1@domain1.com", "grade": 2, "active": false, "data": "value"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		// duplicated unique key
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: "table_not_found", PartitionKeyValues: []interface{}{"user"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "username": "user", "email": "user@domain.com", "grade": 1, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

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
