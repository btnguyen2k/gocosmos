// Package gocosmos provides database/sql driver and a REST API client for Azure Cosmos DB SQL API.
package gocosmos

import (
	"reflect"
)

const (
	// Version of package gocosmos.
	Version = "0.2.1"
)

func goTypeToCosmosDbType(typ reflect.Type) string {
	if typ == nil {
		return ""
	}
	switch typ.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "STRING"
	case reflect.Float32, reflect.Float64:
		return "NUMBER"
	case reflect.Array, reflect.Slice:
		return "ARRAY"
	case reflect.Map:
		return "MAP"
	}
	return ""
}
