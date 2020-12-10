package go_cosmos

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var (
	reValNull        = regexp.MustCompile(`(?i)(null)\s*,?`)
	reValNumber      = regexp.MustCompile(`([\d\.xe+-]+)\s*,?`)
	reValBoolean     = regexp.MustCompile(`(?i)(true|false)\s*,?`)
	reValString      = regexp.MustCompile(`(?i)("(\\"|[^"])*?")\s*,?`)
	reValPlaceholder = regexp.MustCompile(`(?i)[$@:](\d+)\s*,?`)
)

type placeholder struct {
	index int
}

// StmtInsert implements "INSERT" operation.
//
// Syntax: INSERT INTO <db-name>.<collection-name> (<field-list>) VALUES (<value-list>)
//
// - values are comma separated.
// - a value is either:
//   - a placeholder (e.g. :1, @2 or $3)
//   - a null
//   - a number
//   - a boolean (true/false)
//   - a string (inside double quotes) that must be a valid JSON, e.g.
//     - a string value in JSON (include the double quotes): "\"a string\""
//     - a number value in JSON (include the double quotes): "123"
//     - a boolean value in JSON (include the double quotes): "true"
//     - a null value in JSON (include the double quotes): "null"
//     - a map value in JSON (include the double quotes): "{\"key\":\"value\"}"
//     - a list value in JSON (include the double quotes): "[1,true,nil,\"string\"]"
type StmtInsert struct {
	*Stmt
	dbName    string
	collName  string
	fieldsStr string
	valuesStr string
	fields    []string
	values    []interface{}
}

func (s *StmtInsert) parse() error {
	s.fields = regexp.MustCompile(`[,\s]+`).Split(s.fieldsStr, -1)
	s.values = make([]interface{}, 0)
	s.numInput = 0
	for temp := strings.TrimSpace(s.valuesStr); temp != ""; temp = strings.TrimSpace(temp) {
		if loc := reValPlaceholder.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token := strings.Trim(temp[loc[0]+1:loc[1]], " ,")
			s.numInput++
			index, _ := strconv.Atoi(token)
			s.values = append(s.values, placeholder{index})
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValNull.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			s.values = append(s.values, nil)
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValNumber.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token := strings.Trim(temp[loc[0]:loc[1]], " ,")
			var data interface{}
			if err := json.Unmarshal([]byte(token), &data); err != nil {
				return errors.New("(nul) cannot parse query, invalid token at: " + token)
			}
			s.values = append(s.values, data)
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValBoolean.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token := strings.Trim(temp[loc[0]:loc[1]], " ,")
			var data bool
			if err := json.Unmarshal([]byte(token), &data); err != nil {
				return errors.New("(bool) cannot parse query, invalid token at: " + token)
			}
			s.values = append(s.values, data)
			temp = temp[loc[1]:]
			continue
		}
		if loc := reValString.FindStringIndex(temp); loc != nil && loc[0] == 0 {
			token, err := strconv.Unquote(strings.Trim(temp[loc[0]:loc[1]], " ,"))
			if err != nil {
				return errors.New("(unquote) cannot parse query, invalid token at: " + token)
			}
			var data interface{}
			if err := json.Unmarshal([]byte(token), &data); err != nil {
				return errors.New("(unmarshal) cannot parse query, invalid token at: " + token)
			}
			s.values = append(s.values, data)
			temp = temp[loc[1]:]
			continue
		}
		return errors.New("cannot parse query, invalid token at: " + temp)
	}

	return nil
}

func (s *StmtInsert) validate() error {
	return nil
}

// Exec implements driver.Stmt.Exec.
func (s *StmtInsert) Exec(args []driver.Value) (driver.Result, error) {
	panic("implement me")
}

// Query implements driver.Stmt.Query.
func (s *StmtInsert) Query(args []driver.Value) (driver.Rows, error) {
	panic("implement me")
}
