package gocosmos

import (
	"database/sql"
	"testing"
)

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

func TestSelect_DatasetNutrition_SmallRU(t *testing.T) {
	testName := "TestSelect_DatasetNutrition_SmallRU"
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

func TestSelect_DatasetNutrition_LargeRU(t *testing.T) {
	testName := "TestSelect_DatasetNutrition_LargeRU"
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
