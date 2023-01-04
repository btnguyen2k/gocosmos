package gocosmos

import (
	"testing"
)

/*----------------------------------------------------------------------*/

func TestRestClient_CreateDatabase(t *testing.T) {
	name := "TestRestClient_CreateDatabase"
	client := _newRestClient(t, name)

	dbname := testDb
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

	dbname := testDb
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

	dbname := testDb
	client.DeleteDatabase(dbname)
	dbspec := DatabaseSpec{Id: dbname}
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

	dbname := testDb
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

	dbname := testDb
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

	dbnames := []string{"db1", "db2", "db3", "db4", "db5"}
	defer func() {
		for _, dbname := range dbnames {
			client.DeleteDatabase(dbname)
		}
	}()
	dbnamesMap := make(map[string]int)
	for _, dbname := range dbnames {
		dbnamesMap[dbname] = 1
		client.CreateDatabase(DatabaseSpec{Id: dbname, Ru: 400, MaxRu: 0})
	}

	if result := client.ListDatabases(); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if int(result.Count) < len(dbnamesMap) {
		t.Fatalf("%s failed: number of returned databases %#v", name, result.Count)
	} else {
		for _, db := range result.Databases {
			delete(dbnamesMap, db.Id)
		}
		if len(dbnamesMap) != 0 {
			t.Fatalf("%s failed: databases not returned %#v", name, dbnamesMap)
		}
	}
}
