package binding

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// IsBulk reports whether any binding carries an array of values.
func IsBulk(bindings map[string]Binding) bool {
	for _, b := range bindings {
		if isArray(b.Value) {
			return true
		}
	}
	return false
}

func isArray(v json.RawMessage) bool {
	t := strings.TrimSpace(string(v))
	return t != "" && t[0] == '['
}

// Expand turns sql plus bindings into the statements to execute. Scalar
// bindings yield one statement. Array (bulk) bindings run the statement once
// per row: a plain "INSERT ... VALUES (...)" is folded into a single multi-row
// INSERT so it stays atomic, anything else yields one statement per row.
// Scalar bindings mixed with arrays are repeated for every row.
func Expand(sql string, bindings map[string]Binding) ([]string, error) {
	if !IsBulk(bindings) {
		out, err := Substitute(sql, bindings)
		if err != nil {
			return nil, err
		}
		return []string{out}, nil
	}

	rows, err := splitRows(bindings)
	if err != nil {
		return nil, err
	}

	if prefix, tuple, ok := splitInsertValues(sql); ok {
		var b strings.Builder
		b.WriteString(prefix)
		b.WriteString(" VALUES ")
		for i, row := range rows {
			t, err := Substitute(tuple, row)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(t)
		}
		return []string{b.String()}, nil
	}

	stmts := make([]string, len(rows))
	for i, row := range rows {
		if stmts[i], err = Substitute(sql, row); err != nil {
			return nil, err
		}
	}
	return stmts, nil
}

// splitRows pivots array bindings into one scalar binding map per row.
func splitRows(bindings map[string]Binding) ([]map[string]Binding, error) {
	keys := make([]string, 0, len(bindings))
	for k := range bindings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	n := -1
	cols := make(map[string][]json.RawMessage)
	for _, k := range keys {
		b := bindings[k]
		if !isArray(b.Value) {
			continue
		}
		var vals []json.RawMessage
		if err := json.Unmarshal(b.Value, &vals); err != nil {
			return nil, fmt.Errorf("invalid bind array for variable %s", k)
		}
		if n >= 0 && len(vals) != n {
			return nil, fmt.Errorf("bind arrays must all have the same length (variable %s has %d, expected %d)", k, len(vals), n)
		}
		n = len(vals)
		cols[k] = vals
	}
	if n == 0 {
		return nil, fmt.Errorf("bind arrays must not be empty")
	}

	rows := make([]map[string]Binding, n)
	for r := range rows {
		row := make(map[string]Binding, len(bindings))
		for k, b := range bindings {
			if vals, ok := cols[k]; ok {
				row[k] = Binding{Type: b.Type, Value: vals[r]}
			} else {
				row[k] = b
			}
		}
		rows[r] = row
	}
	return rows, nil
}

// splitInsertValues splits "INSERT ... VALUES (<tuple>)" into everything
// before VALUES and the single trailing tuple. It reports false for anything
// else (INSERT ... SELECT, multiple tuples, placeholders outside the tuple).
func splitInsertValues(sql string) (prefix, tuple string, ok bool) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	if len(sql) < 6 || !strings.EqualFold(sql[:6], "INSERT") {
		return "", "", false
	}

	depth, valuesAt := 0, -1
	for i := 0; i < len(sql) && valuesAt < 0; {
		if j, special := skipSpecial(sql, i); special {
			i = j
			continue
		}
		switch c := sql[i]; {
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (c == 'v' || c == 'V') && (i == 0 || !isIdentChar(sql[i-1])) &&
			len(sql) >= i+6 && strings.EqualFold(sql[i:i+6], "VALUES") &&
			(len(sql) == i+6 || !isIdentChar(sql[i+6])):
			valuesAt = i
		}
		i++
	}
	if valuesAt < 0 {
		return "", "", false
	}

	prefix = strings.TrimSpace(sql[:valuesAt])
	tuple = strings.TrimSpace(sql[valuesAt+6:])
	if !strings.HasPrefix(tuple, "(") || hasPlaceholder(prefix) {
		return "", "", false
	}

	// The tuple must be one balanced (...) group spanning the rest of the text.
	depth = 0
	for i := 0; i < len(tuple); {
		if j, special := skipSpecial(tuple, i); special {
			i = j
			continue
		}
		switch tuple[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(tuple)-1 {
				return "", "", false
			}
		}
		i++
	}
	if depth != 0 {
		return "", "", false
	}
	return prefix, tuple, true
}

func hasPlaceholder(sql string) bool {
	found := false
	_, _ = render(sql, func(int) (string, error) {
		found = true
		return strconv.Quote(""), nil
	})
	return found
}

// FillNull replaces every placeholder with NULL. It is used for describe-only
// requests, where drivers send the statement without any bind values.
func FillNull(sql string) string {
	out, _ := render(sql, func(int) (string, error) { return "NULL", nil })
	return out
}
