package gocosmos_test

import (
	"encoding/json"
	"fmt"
	"github.com/microsoft/gocosmos"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

/*======================================================================*/

const numApps = 4
const numLogicalPartitions = 16
const numCategories = 19

var dataList []gocosmos.DocInfo

func _initDataSubPartitions(t *testing.T, testName string, client *gocosmos.RestClient, db, container string, numItem int) {
	totalRu := 0.0
	randList := make([]int, numItem)
	for i := 0; i < numItem; i++ {
		randList[i] = i*2 + 1
	}
	rand.Shuffle(numItem, func(i, j int) {
		randList[i], randList[j] = randList[j], randList[i]
	})
	dataList = make([]gocosmos.DocInfo, numItem)
	for i := 0; i < numItem; i++ {
		category := randList[i] % numCategories
		app := "app" + strconv.Itoa(i%numApps)
		username := "user" + strconv.Itoa(i%numLogicalPartitions)
		docInfo := gocosmos.DocInfo{
			"id":       fmt.Sprintf("%05d", i),
			"app":      app,
			"username": username,
			"email":    "user" + strconv.Itoa(i) + "@domain.com",
			"grade":    float64(randList[i]),
			"category": float64(category),
			"active":   i%10 == 0,
			"big":      fmt.Sprintf("%05d", i) + "/" + strings.Repeat("this is a very long string/", 256),
		}
		dataList[i] = docInfo
		if result := client.CreateDocument(gocosmos.DocumentSpec{DbName: db, CollName: container, PartitionKeyValues: []interface{}{app, username}, DocumentData: docInfo}); result.Error() != nil {
			t.Fatalf("%s failed: %s", testName, result.Error())
		} else {
			totalRu += result.RequestCharge
		}
	}
}

func _initDataSubPartitionsSmallRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string, numItem int) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 400})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               400,
	})
	_initDataSubPartitions(t, testName, client, db, container, numItem)
}

func _initDataSubPartitionsLargeRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string, numItem int) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 20000})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/app", "/username"}, "kind": "MultiHash", "version": 2},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               20000,
	})
	_initDataSubPartitions(t, testName, client, db, container, numItem)
}

func _initData(t *testing.T, testName string, client *gocosmos.RestClient, db, container string, numItem int) {
	totalRu := 0.0
	randList := make([]int, numItem)
	for i := 0; i < numItem; i++ {
		randList[i] = i*2 + 1
	}
	rand.Shuffle(numItem, func(i, j int) {
		randList[i], randList[j] = randList[j], randList[i]
	})
	dataList = make([]gocosmos.DocInfo, numItem)
	for i := 0; i < numItem; i++ {
		category := randList[i] % numCategories
		username := "user" + strconv.Itoa(i%numLogicalPartitions)
		docInfo := gocosmos.DocInfo{
			"id":       fmt.Sprintf("%05d", i),
			"username": username,
			"email":    "user" + strconv.Itoa(i) + "@domain.com",
			"grade":    float64(randList[i]),
			"category": float64(category),
			"active":   i%10 == 0,
			"big":      fmt.Sprintf("%05d", i) + "/" + strings.Repeat("this is a very long string/", 256),
		}
		dataList[i] = docInfo
		if result := client.CreateDocument(gocosmos.DocumentSpec{DbName: db, CollName: container, PartitionKeyValues: []interface{}{username}, DocumentData: docInfo}); result.Error() != nil {
			t.Fatalf("%s failed: %s", testName, result.Error())
		} else {
			totalRu += result.RequestCharge
		}
	}
}

func _initDataSmallRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string, numItem int) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 400})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               400,
	})
	_initData(t, testName, client, db, container, numItem)
}

func _initDataLargeRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string, numItem int) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 20000})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/username"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               20000,
	})
	_initData(t, testName, client, db, container, numItem)
}

/*----------------------------------------------------------------------*/
func _loadTestDataFromFile(filepath string) ([]byte, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}

/*----------------------------------------------------------------------*/

func _initDataFamilies(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_testDataFamilies, err := _loadTestDataFromFile("testdata/families.json")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/load-test-data", err)
	}
	dataMapFamilies = make(map[string]gocosmos.DocInfo)
	dataListFamilies = make([]gocosmos.DocInfo, 0)
	err = json.Unmarshal(_testDataFamilies, &dataListFamilies)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	for _, doc := range dataListFamilies {
		if result := client.CreateDocument(gocosmos.DocumentSpec{DbName: db, CollName: container, PartitionKeyValues: []interface{}{doc["id"]}, DocumentData: doc}); result.Error() != nil {
			t.Fatalf("%s failed: %s", testName, result.Error())
		}
		dataMapFamilies[doc.Id()] = doc
	}
}

func _initDataFamiliesSmallRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 400})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		Ru:               400,
	})
	_initDataFamilies(t, testName, client, db, container)
}

func _initDataFamiliesLargeRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 20000})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               20000,
	})
	_initDataFamilies(t, testName, client, db, container)
}

func _initDataVolcano(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_testDataVolcano, err := _loadTestDataFromFile("testdata/volcano.json")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/load-test-data", err)
	}
	dataListVolcano = make([]gocosmos.DocInfo, 0)
	err = json.Unmarshal(_testDataVolcano, &dataListVolcano)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	for _, doc := range dataListVolcano {
		if result := client.CreateDocument(gocosmos.DocumentSpec{DbName: db, CollName: container, PartitionKeyValues: []interface{}{doc["id"]}, DocumentData: doc}); result.Error() != nil {
			t.Fatalf("%s failed: %s", testName, result.Error())
		}
	}
}

func _initDataVolcanoSmallRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 400})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		Ru:               400,
	})
	_initDataVolcano(t, testName, client, db, container)
}

func _initDataVolcanoLargeRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 20000})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               20000,
	})
	_initDataVolcano(t, testName, client, db, container)
}

var dataListFamilies, dataListVolcano []gocosmos.DocInfo
var dataMapFamilies map[string]gocosmos.DocInfo

func _toJson(data interface{}) string {
	js, _ := json.Marshal(data)
	return string(js)
}

/*----------------------------------------------------------------------*/

func _initDataNutrition(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_testDataNutrition, err := _loadTestDataFromFile("testdata/nutrition.json")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/load-test-data", err)
	}
	dataListNutrition := make([]gocosmos.DocInfo, 0)
	dataMapNutrition := sync.Map{}
	err = json.Unmarshal(_testDataNutrition, &dataListNutrition)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	fmt.Printf("\tDataset: %#v / Number of records: %#v\n", "Nutrition", len(dataListNutrition))

	numWorkers := 4
	buff := make(chan gocosmos.DocInfo, numWorkers*4)
	wg := &sync.WaitGroup{}
	wg.Add(numWorkers)
	numDocWritten := int64(0)
	start := time.Now()
	for id := 0; id < numWorkers; id++ {
		go func(id int, wg *sync.WaitGroup, buff <-chan gocosmos.DocInfo) {
			defer wg.Done()
			for doc := range buff {
				docId := doc["id"].(string)
				dataMapNutrition.Store(docId, doc)
				if result := client.CreateDocument(gocosmos.DocumentSpec{DbName: db, CollName: container, PartitionKeyValues: []interface{}{docId}, DocumentData: doc}); result.Error() != nil {
					t.Errorf("%s failed: (%#v) %s", testName, id, result.Error())
					return
				}
				atomic.AddInt64(&numDocWritten, 1)
				for {
					now := time.Now()
					d := now.Sub(start)
					r := float64(numDocWritten) / (d.Seconds() + 0.001)
					if r <= 81.19 {
						break
					}
					fmt.Printf("\t[DEBUG] too fast, slowing down...(Id: %d / NumDocs: %d / Dur: %.3f / Rate: %.3f)\n", id, numDocWritten, d.Seconds(), r)
					time.Sleep(1*time.Second + time.Duration(rand.Intn(1234))*time.Millisecond)
				}
			}
			fmt.Printf("\t\tWorker %#v: %#v docs written\n", id, numDocWritten)
		}(id, wg, buff)
	}
	for _, doc := range dataListNutrition {
		buff <- doc
	}
	close(buff)
	wg.Wait()
	{
		now := time.Now()
		d := now.Sub(start)
		r := float64(numDocWritten) / (d.Seconds() + 0.001)
		fmt.Printf("\t[DEBUG] Dur: %.3f / Rate: %.3f\n", d.Seconds(), r)
		time.Sleep(1*time.Second + time.Duration(rand.Intn(1234))*time.Millisecond)
	}
	count := 0
	dataMapNutrition.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	fmt.Printf("\tDataset: %#v / (checksum) Number of records: %#v\n", "Nutrition", count)
}

func _initDataNutritionSmallRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 400})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		Ru:               400,
	})
	_initDataNutrition(t, testName, client, db, container)
}

func _initDataNutritionLargeRU(t *testing.T, testName string, client *gocosmos.RestClient, db, container string) {
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: db, Ru: 20000})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           db,
		CollName:         container,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		UniqueKeyPolicy:  map[string]interface{}{"uniqueKeys": []map[string]interface{}{{"paths": []string{"/email"}}}},
		Ru:               20000,
	})
	_initDataNutrition(t, testName, client, db, container)
}
