package fold

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/xuri/excelize/v2"
)

// RawRecord is the intermediate representation returned by Excel and JSONL readers.
type RawRecord map[string]any

// Str returns a string value.
func (r RawRecord) Str(field string) string {
	v, ok := r[field]
	if !ok || v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case []string:
		if len(s) > 0 {
			return s[0]
		}
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// StrList returns a string list value.
func (r RawRecord) StrList(field string) []string {
	v, ok := r[field]
	if !ok || v == nil {
		return nil
	}
	switch s := v.(type) {
	case []string:
		return s
	case string:
		if s == "" {
			return nil
		}
		return []string{s}
	default:
		return []string{fmt.Sprintf("%v", v)}
	}
}

// Int64 returns an int64 value.
func (r RawRecord) Int64(field string) int64 {
	v, ok := r[field]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

// Match reports whether a string field matches a regular expression.
func (r RawRecord) Match(field, pattern string) bool {
	v := r.Str(field)
	if v == "" {
		return false
	}
	matched, _ := regexp.MatchString(pattern, v)
	return matched
}

// ExcelOpt configures Excel parsing.
type ExcelOpt struct {
	Sheet     string                   // sheet name; defaults to the first sheet
	Header    int                      // 1-based absolute header row number
	Fields    map[string]string        // target field to column expression
	Null      []string                 // global null-like values
	Split     map[string]string        // target field to separator
	Transform map[string]func(any) any // target field to cleaning function, applied after Split
}

// ReadExcel parses an Excel file into RawRecord rows.
// Field column expressions support two operators:
//   - &: merge all non-empty columns into []string
//   - |: select the first non-empty column value
func ReadExcel(path string, opt *ExcelOpt) ([]RawRecord, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, fmt.Errorf("fold: open Excel: %w", err)
	}
	defer f.Close()

	sheet := opt.Sheet
	if sheet == "" {
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return nil, fmt.Errorf("fold: Excel file has no sheets")
		}
		sheet = sheets[0]
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("fold: read sheet %s: %w", sheet, err)
	}

	headerIdx := opt.Header - 1
	if headerIdx < 0 || headerIdx >= len(rows) {
		return nil, fmt.Errorf("fold: header row %d out of range (%d rows)", opt.Header, len(rows))
	}

	headerRow := rows[headerIdx]
	colIndex := make(map[string]int)
	for i, h := range headerRow {
		h = strings.TrimSpace(h)
		if h != "" {
			colIndex[h] = i
		}
	}

	nullSet := make(map[string]bool)
	for _, n := range opt.Null {
		nullSet[strings.TrimSpace(n)] = true
	}

	type fieldExpr struct {
		target string
		op     string
		cols   []string
	}
	var exprs []fieldExpr
	for target, expr := range opt.Fields {
		if strings.Contains(expr, "&") {
			cols := splitTrim(expr, "&")
			exprs = append(exprs, fieldExpr{target: target, op: "&", cols: cols})
		} else if strings.Contains(expr, "|") {
			cols := splitTrim(expr, "|")
			exprs = append(exprs, fieldExpr{target: target, op: "|", cols: cols})
		} else {
			exprs = append(exprs, fieldExpr{target: target, op: "", cols: []string{strings.TrimSpace(expr)}})
		}
	}

	var records []RawRecord
	for rowIdx := headerIdx + 1; rowIdx < len(rows); rowIdx++ {
		row := rows[rowIdx]
		rec := make(RawRecord)

		for _, fe := range exprs {
			switch fe.op {
			case "&":
				var vals []string
				for _, col := range fe.cols {
					idx, ok := colIndex[col]
					if !ok || idx >= len(row) {
						continue
					}
					v := strings.TrimSpace(row[idx])
					if v != "" && !nullSet[v] {
						vals = append(vals, v)
					}
				}
				if len(vals) > 0 {
					rec[fe.target] = vals
				}
			case "|":
				for _, col := range fe.cols {
					idx, ok := colIndex[col]
					if !ok || idx >= len(row) {
						continue
					}
					v := strings.TrimSpace(row[idx])
					if v != "" && !nullSet[v] {
						rec[fe.target] = v
						break
					}
				}
			default:
				col := fe.cols[0]
				idx, ok := colIndex[col]
				if !ok || idx >= len(row) {
					continue
				}
				v := strings.TrimSpace(row[idx])
				if v != "" && !nullSet[v] {
					rec[fe.target] = v
				}
			}
		}

		for field, sep := range opt.Split {
			v, ok := rec[field]
			if !ok {
				continue
			}
			rec[field] = applySplit(v, sep, nullSet)
		}

		for field, tf := range opt.Transform {
			v, ok := rec[field]
			if !ok {
				continue
			}
			if result := tf(v); result != nil {
				rec[field] = result
			} else {
				delete(rec, field)
			}
		}

		if len(rec) > 0 {
			records = append(records, rec)
		}
	}

	return records, nil
}

// JSONLOpt configures JSONL parsing.
type JSONLOpt struct {
	Root      string                   // nested JSON path such as "data.items"
	Fields    map[string]string        // target field to gjson path expression
	Null      []string                 // global null-like values
	Split     map[string]string        // target field to separator
	Transform map[string]func(any) any // target field to cleaning function, applied after Split
}

// ReadJSONL parses a JSONL file into RawRecord rows.
func ReadJSONL(path string, opt *JSONLOpt) ([]RawRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fold: open JSONL: %w", err)
	}
	defer f.Close()

	nullSet := make(map[string]bool)
	for _, n := range opt.Null {
		nullSet[strings.TrimSpace(n)] = true
	}

	var records []RawRecord
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		jsonStr := line
		if opt.Root != "" {
			result := gjson.Get(line, opt.Root)
			if !result.Exists() {
				continue
			}
			if result.IsArray() {
				result.ForEach(func(_, item gjson.Result) bool {
					rec := extractJSONLRecord(item.Raw, opt.Fields, nullSet)
					applyJSONLSplit(rec, opt.Split, nullSet)
					applyTransform(rec, opt.Transform)
					if len(rec) > 0 {
						records = append(records, rec)
					}
					return true
				})
				continue
			}
			jsonStr = result.Raw
		}

		rec := extractJSONLRecord(jsonStr, opt.Fields, nullSet)
		applyJSONLSplit(rec, opt.Split, nullSet)
		applyTransform(rec, opt.Transform)
		if len(rec) > 0 {
			records = append(records, rec)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("fold: read JSONL: %w", err)
	}

	return records, nil
}

func extractJSONLRecord(jsonStr string, fields map[string]string, nullSet map[string]bool) RawRecord {
	rec := make(RawRecord)
	for target, expr := range fields {
		if strings.Contains(expr, "&") {
			var vals []string
			for _, p := range splitTrim(expr, "&") {
				result := gjson.Get(jsonStr, p)
				if result.Exists() {
					v := strings.TrimSpace(result.String())
					if v != "" && !nullSet[v] {
						vals = append(vals, v)
					}
				}
			}
			if len(vals) > 0 {
				rec[target] = vals
			}
		} else if strings.Contains(expr, "|") {
			for _, p := range splitTrim(expr, "|") {
				result := gjson.Get(jsonStr, p)
				if result.Exists() {
					v := strings.TrimSpace(result.String())
					if v != "" && !nullSet[v] {
						rec[target] = v
						break
					}
				}
			}
		} else {
			p := strings.TrimSpace(expr)
			result := gjson.Get(jsonStr, p)
			if result.Exists() {
				v := strings.TrimSpace(result.String())
				if v != "" && !nullSet[v] {
					rec[target] = v
				}
			}
		}
	}
	return rec
}

func applyTransform(rec RawRecord, transforms map[string]func(any) any) {
	for field, tf := range transforms {
		v, ok := rec[field]
		if !ok {
			continue
		}
		if result := tf(v); result != nil {
			rec[field] = result
		} else {
			delete(rec, field)
		}
	}
}

func applyJSONLSplit(rec RawRecord, splitRules map[string]string, nullSet map[string]bool) {
	for field, sep := range splitRules {
		v, ok := rec[field]
		if !ok {
			continue
		}
		rec[field] = applySplit(v, sep, nullSet)
	}
}

func applySplit(v any, sep string, nullSet map[string]bool) any {
	var parts []string
	switch s := v.(type) {
	case string:
		parts = strings.Split(s, sep)
	case []string:
		for _, item := range s {
			parts = append(parts, strings.Split(item, sep)...)
		}
	default:
		return v
	}

	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" && !nullSet[p] {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func splitTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
