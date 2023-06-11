package gocosmos

import (
	"strings"
	"testing"
	"time"
)

/*----------------------------------------------------------------------*/

func TestRestClient_CreateDocument_SubPartitions(t *testing.T) {
	name := "TestRestClient_CreateDocument_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app1", "user1"},
		DocumentData: map[string]interface{}{"id": "1", "app": "app1", "username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "1" || result.DocInfo["app"] != "app1" || result.DocInfo["username"] != "user1" ||
		result.DocInfo["email"] != "user1@domain.com" || result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app1", "user11"}, IndexingDirective: "Include",
		DocumentData: map[string]interface{}{"id": "11", "app": "app1", "username": "user11", "email": "user11@domain.com", "grade": 1.1, "active": false},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "11" || result.DocInfo["app"] != "app1" || result.DocInfo["username"] != "user11" ||
		result.DocInfo["email"] != "user11@domain.com" || result.DocInfo["grade"].(float64) != 1.1 || result.DocInfo["active"] != false ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app2", "user111"}, IndexingDirective: "Exclude",
		DocumentData: map[string]interface{}{"id": "111", "app": "app2", "username": "user111", "email": "user111@domain.com", "grade": 1.11, "active": false},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "111" || result.DocInfo["app"] != "app2" || result.DocInfo["username"] != "user111" ||
		result.DocInfo["email"] != "user111@domain.com" || result.DocInfo["grade"].(float64) != 1.11 || result.DocInfo["active"] != false ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// duplicated id
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app1", "user1"},
		DocumentData: map[string]interface{}{"id": "1", "app": "app1", "username": "user1", "email": "user@domain1.com", "grade": 2, "active": false},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}

	// duplicated unique index
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app1", "user1"},
		DocumentData: map[string]interface{}{"id": "2", "app": "app1", "username": "user1", "email": "user1@domain.com", "grade": 3, "active": true},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}
}

func TestRestClient_CreateDocumentNoId_SubPartitions(t *testing.T) {
	name := "TestRestClient_CreateDocumentNoId_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
	})

	client.autoId = true
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"},
		DocumentData: map[string]interface{}{"app": "app", "username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] == "" || result.DocInfo["app"] != "app" || result.DocInfo["username"] != "user1" || result.DocInfo["email"] != "user1@domain.com" ||
		result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true || result.DocInfo["_rid"] == "" ||
		result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	client.autoId = false
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user2"},
		DocumentData: map[string]interface{}{"app": "app", "username": "user2", "email": "user2@domain.com", "grade": 2, "active": false},
	}); result.Error() == nil {
		t.Fatalf("%s failed: this operation should not be successful", name)
	}
}

func TestRestClient_UpsertDocument_SubPartitions(t *testing.T) {
	name := "TestRestClient_UpsertDocument_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "app": "app", "username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "1" || result.DocInfo["app"] != "app" || result.DocInfo["username"] != "user1" ||
		result.DocInfo["email"] != "user1@domain.com" || result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user2"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "2", "app": "app", "username": "user2", "email": "user2@domain.com", "grade": 2, "active": false},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "2" || result.DocInfo["app"] != "app" || result.DocInfo["username"] != "user2" ||
		result.DocInfo["email"] != "user2@domain.com" || result.DocInfo["grade"].(float64) != 2.0 || result.DocInfo["active"] != false ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "1", "app": "app", "username": "user1", "email": "user1@domain1.com", "grade": 2, "active": false, "data": "value"},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != "1" || result.DocInfo["app"] != "app" || result.DocInfo["username"] != "user1" ||
		result.DocInfo["email"] != "user1@domain1.com" || result.DocInfo["grade"].(float64) != 2.0 || result.DocInfo["active"] != false || result.DocInfo["data"] != "value" ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// duplicated unique key
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"id": "3", "app": "app", "username": "user1", "email": "user1@domain1.com", "grade": 2, "active": false, "data": "value"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
	}
}

func TestRestClient_UpsertDocumentNoId_SubPartitions(t *testing.T) {
	name := "TestRestClient_UpsertDocumentNoId_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	client.autoId = true
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"app": "app", "username": "user1", "email": "user1@domain.com", "grade": 1, "active": true},
	}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] == "" || result.DocInfo["app"] != "app" || result.DocInfo["username"] != "user1" ||
		result.DocInfo["email"] != "user1@domain.com" || result.DocInfo["grade"].(float64) != 1.0 || result.DocInfo["active"] != true ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	client.autoId = false
	if result := client.CreateDocument(DocumentSpec{
		DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user2"}, IsUpsert: true,
		DocumentData: map[string]interface{}{"app": "app", "username": "user2", "email": "user2@domain.com", "grade": 2, "active": false},
	}); result.Error() == nil {
		t.Fatalf("%s failed: this operation should not be successful", name)
	}
}

func TestRestClient_ReplaceDocument_SubPartitions(t *testing.T) {
	name := "TestRestClient_ReplaceDocument_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	// insert 2 documents
	docInfo := map[string]interface{}{"id": "2", "app": "app", "username": "user", "email": "user2@domain.com", "grade": 2.0, "active": false}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}
	docInfo = map[string]interface{}{"id": "1", "app": "app", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// conflict unique key
	docInfo["email"] = "user2@domain.com"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 409 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	// replace document without etag matching
	var etag string
	docInfo = map[string]interface{}{"id": "1", "app": "app", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	docInfo["grade"] = 2.0
	docInfo["active"] = false
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	} else {
		etag = result.DocInfo["_etag"].(string)
	}

	// replace document with etag matching: should not match
	docInfo["email"] = "user3@domain.com"
	docInfo["grade"] = 3.0
	docInfo["active"] = true
	if result := client.ReplaceDocument(etag+"dummy", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 412 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 412, result.StatusCode)
	}
	// replace document with etag matching: should match
	if result := client.ReplaceDocument(etag, DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	// document not found
	docInfo["id"] = "0"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ReplaceDocumentCrossPartitions_SubPartitions(t *testing.T) {
	name := "TestRestClient_ReplaceDocumentCrossPartitions_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	// insert a document
	docInfo := map[string]interface{}{"id": "1", "app": "app", "username": "user1", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	docInfo["username"] = "user2"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user2"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	docInfo["username"] = "user2"
	if result := client.ReplaceDocument("", DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user1"}, DocumentData: docInfo}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 400 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 400, result.StatusCode)
	}
}

func TestRestClient_GetDocument_SubPartitions(t *testing.T) {
	name := "TestRestClient_GetDocument_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	var etag, sessionToken string
	docInfo := map[string]interface{}{"id": "1", "app": "app", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		etag = result.DocInfo["_etag"].(string)
		sessionToken = result.SessionToken
	}

	if result := client.GetDocument(DocReq{DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"app", "user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo.Id() != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo.Rid() == "" || result.DocInfo.Self() == "" || result.DocInfo.Ts() == 0 || result.DocInfo.Etag() == "" || result.DocInfo.Attachments() == "" {
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
		PartitionKeyValues: []interface{}{"app", "user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.DocInfo["id"] != docInfo["id"] || result.DocInfo["app"] != docInfo["app"] || result.DocInfo["username"] != docInfo["username"] ||
		result.DocInfo["email"] != docInfo["email"] || result.DocInfo["grade"] != docInfo["grade"] || result.DocInfo["active"] != docInfo["active"] ||
		result.DocInfo["_rid"] == "" || result.DocInfo["_self"] == "" || result.DocInfo["_ts"].(float64) == 0.0 || result.DocInfo["_etag"] == "" || result.DocInfo["_attachments"] == "" {
		t.Fatalf("%s failed: invalid dbinfo returned %#v", name, result.DocInfo)
	}

	if result := client.GetDocument(DocReq{NotMatchEtag: etag, DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"app", "user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 304 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 304, result.StatusCode)
	}

	if result := client.GetDocument(DocReq{DbName: dbname, CollName: collname, DocId: "0", PartitionKeyValues: []interface{}{"app", "user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_DeleteDocument_SubPartitions(t *testing.T) {
	name := "TestRestClient_DeleteDocument_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	client.DeleteDatabase(dbname)
	client.CreateDatabase(DatabaseSpec{Id: dbname})
	client.CreateCollection(CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
	})

	var etag string
	docInfo := map[string]interface{}{"id": "1", "app": "app", "username": "user", "email": "user1@domain.com", "grade": 1.0, "active": true}
	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		etag = result.DocInfo["_etag"].(string)
	}

	if result := client.DeleteDocument(DocReq{MatchEtag: etag + "dummy", DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"app", "user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 412 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
	if result := client.DeleteDocument(DocReq{MatchEtag: etag, DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"app", "user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}

	if result := client.CreateDocument(DocumentSpec{DbName: dbname, CollName: collname, PartitionKeyValues: []interface{}{"app", "user"}, DocumentData: docInfo}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"app", "user"}}); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}

	if result := client.DeleteDocument(DocReq{DbName: dbname, CollName: collname, DocId: "1", PartitionKeyValues: []interface{}{"app", "user"}}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}
