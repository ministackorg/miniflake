package rewriter

import (
	"regexp"
	"strconv"
	"strings"
)

// Date/time rewrites (#34): the niladic functions in their parenthesized
// form, CONVERT_TIMEZONE, the TIMESTAMP_NTZ/LTZ/TZ type names outside DDL,
// TO_TIMESTAMP_* and the format argument of TO_CHAR / TO_VARCHAR / TO_DATE /
// TO_TIMESTAMP.

// replaceOutsideStrings applies re/fn to the parts of sql that are not inside
// single-quoted string literals.
func replaceOutsideStrings(sql string, re *regexp.Regexp, fn func(string) string) string {
	var out strings.Builder
	seg := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '\'' {
			continue
		}
		out.WriteString(re.ReplaceAllStringFunc(sql[seg:i], fn))
		j := i + 1
		for j < len(sql) {
			if sql[j] == '\'' {
				if j+1 < len(sql) && sql[j+1] == '\'' {
					j += 2
					continue
				}
				break
			}
			j++
		}
		if j >= len(sql) {
			out.WriteString(sql[i:])
			return out.String()
		}
		out.WriteString(sql[i : j+1])
		i = j
		seg = j + 1
	}
	out.WriteString(re.ReplaceAllStringFunc(sql[seg:], fn))
	return out.String()
}

// localTime is the session-local current time as a TIME. DuckDB's own
// CURRENT_TIME is a TIME WITH TIME ZONE, and it cannot cast TIMESTAMPTZ to
// TIME directly.
const localTime = "CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS TIME)"

// sysdate is SYSDATE(): the current UTC wall-clock time as TIMESTAMP_NTZ.
const sysdate = "timezone('UTC', CURRENT_TIMESTAMP)"

var reBareCurrentTime = regexp.MustCompile(`(?i)\b(?:CURRENT_TIME|LOCALTIME)\b`)

// rewriteNiladicDateTime handles CURRENT_TIMESTAMP(), CURRENT_TIME(),
// LOCALTIMESTAMP(), LOCALTIME(), SYSDATE(), GETDATE() and SYSTIMESTAMP().
// Snowflake documents the parenthesized forms (with an optional fractional
// seconds precision); DuckDB only knows the bare keywords. The precision is
// dropped: DuckDB timestamps always carry microseconds.
func rewriteNiladicDateTime(sql string) string {
	for _, name := range []string{"CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "GETDATE", "SYSTIMESTAMP"} {
		sql = replaceFuncCall(sql, name, func([]string) string { return "CURRENT_TIMESTAMP" })
	}
	sql = replaceFuncCall(sql, "SYSDATE", func([]string) string { return sysdate })
	// CURRENT_TIME(...) / LOCALTIME(...) first, then the bare keywords.
	const placeholder = "\x00MF_LOCAL_TIME\x00"
	for _, name := range []string{"CURRENT_TIME", "LOCALTIME"} {
		sql = replaceFuncCall(sql, name, func([]string) string { return placeholder })
	}
	sql = replaceOutsideStrings(sql, reBareCurrentTime, func(string) string { return placeholder })
	return strings.ReplaceAll(sql, placeholder, localTime)
}

// rewriteConvertTimezone maps both forms of CONVERT_TIMEZONE onto DuckDB's
// timezone():
//
//	CONVERT_TIMEZONE(src_tz, tgt_tz, ts_ntz) -> TIMESTAMP_NTZ: the wall clock
//	  of ts read in src_tz, shown in tgt_tz.
//	CONVERT_TIMEZONE(tgt_tz, ts) -> the instant ts as wall-clock time in
//	  tgt_tz. Snowflake returns a TIMESTAMP_TZ carrying tgt_tz's offset;
//	  DuckDB timestamps hold no per-value offset, so the result is that wall
//	  clock as TIMESTAMP_NTZ. An NTZ input is read in the session time zone.
func rewriteConvertTimezone(sql string) string {
	return replaceFuncCall(sql, "CONVERT_TIMEZONE", func(args []string) string {
		switch len(args) {
		case 3:
			return "timezone(" + args[1] + ", timezone(" + args[0] + ", CAST(" + args[2] + " AS TIMESTAMP)))"
		case 2:
			return "timezone(" + args[0] + ", CAST(" + args[1] + " AS TIMESTAMPTZ))"
		}
		return "CONVERT_TIMEZONE(" + strings.Join(args, ", ") + ")"
	})
}

// Timestamp type names. TIMESTAMP_NTZ maps to TIMESTAMP and the LTZ / TZ
// variants to TIMESTAMPTZ, wherever a type can appear (::, CAST, TRY_CAST,
// column definitions). A precision such as TIMESTAMP_NTZ(9) is dropped.
var reTimestampTypes = regexp.MustCompile(`(?i)\bTIMESTAMP_(NTZ|LTZ|TZ)\b(\s*\(\s*\d\s*\))?`)

// Snowflake accepts a space between the time and its UTC offset
// ('2026-01-02 00:00:00 +00:00'); DuckDB does not.
var reOffsetLiteral = regexp.MustCompile(`'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)\s+([+-]\d{2}(?::?\d{2})?)'`)

// VARIANT in a cast (::VARIANT, AS VARIANT) is JSON, as in DDL.
var reVariantCast = regexp.MustCompile(`(?i)(::\s*|\bAS\s+)VARIANT\b`)

func rewriteTimestampTypes(sql string) string {
	sql = replaceOutsideStrings(sql, reTimestampTypes, func(m string) string {
		if strings.EqualFold(reTimestampTypes.FindStringSubmatch(m)[1], "NTZ") {
			return "TIMESTAMP"
		}
		return "TIMESTAMPTZ"
	})
	sql = replaceOutsideStrings(sql, reVariantCast, func(m string) string {
		return reVariantCast.FindStringSubmatch(m)[1] + "JSON"
	})
	return reOffsetLiteral.ReplaceAllString(sql, "'$1$2'")
}

// rewriteToTimestampVariants handles TO_TIMESTAMP_NTZ / _LTZ / _TZ, with or
// without a format argument.
func rewriteToTimestampVariants(sql string) string {
	for _, v := range []struct{ name, typ string }{
		{"TO_TIMESTAMP_NTZ", "TIMESTAMP"},
		{"TO_TIMESTAMP_LTZ", "TIMESTAMPTZ"},
		{"TO_TIMESTAMP_TZ", "TIMESTAMPTZ"},
	} {
		typ := v.typ
		sql = replaceFuncCall(sql, v.name, func(args []string) string {
			if len(args) < 1 {
				return v.name + "()"
			}
			if len(args) >= 2 {
				if fmt, ok := strptimeFormat(args[1]); ok {
					return "CAST(strptime(" + args[0] + ", " + fmt + ") AS " + typ + ")"
				}
			}
			return "CAST(" + args[0] + " AS " + typ + ")"
		})
	}
	return sql
}

// ---------------------------------------------------------------------------
// Snowflake date/time format strings
// ---------------------------------------------------------------------------

// A format piece is either strftime text or a fractional-seconds field that
// strftime cannot print at that precision.
type formatPiece struct {
	strftime string // strftime directives / literal text (% escaped)
	fracLen  int    // >0: FFn with n digits; -1: TZH:TZM-style offset
	offset   string // offset layout for fracLen == -1: "TZH:TZM", "TZHTZM", "TZH", "TZM"
}

// formatElements are Snowflake's date/time format elements (case-insensitive),
// longest first so that e.g. MMMM wins over MM and HH24 over HH.
var formatElements = []struct {
	elem     string
	strftime string
}{
	{"TZH:TZM", ""}, {"TZHTZM", ""}, {"TZH", ""}, {"TZM", ""},
	{"YYYY", "%Y"}, {"UUUU", "%Y"}, {"MMMM", "%B"}, {"HH24", "%H"}, {"HH12", "%I"},
	{"H24", "%-H"}, {"H12", "%-I"}, {"MON", "%b"}, {"YY", "%y"}, {"MM", "%m"},
	{"MO", "%-m"}, {"MI", "%M"}, {"ME", "%-M"}, {"DD", "%d"}, {"DY", "%a"},
	{"HH", "%H"}, {"SS", "%S"}, {"AM", "%p"}, {"PM", "%p"},
	{"FF", ""}, // FF[0-9], handled below
	{"Y", "%-y"}, {"D", "%-d"}, {"H", "%-H"}, {"S", "%-S"},
}

// parseDateFormat translates a Snowflake format string (the contents of the
// literal) into pieces. ok is false when the string is not a date/time
// format, e.g. a numeric one such as '999,999.00'.
func parseDateFormat(f string) (pieces []formatPiece, ok bool) {
	var text strings.Builder
	flush := func() {
		if text.Len() > 0 {
			pieces = append(pieces, formatPiece{strftime: text.String()})
			text.Reset()
		}
	}
	sawElement := false
	upper := strings.ToUpper(f)
	for i := 0; i < len(f); {
		if f[i] == '"' { // literal text
			end := strings.IndexByte(f[i+1:], '"')
			if end < 0 {
				end = len(f) - i - 1
			}
			text.WriteString(strings.ReplaceAll(f[i+1:i+1+end], "%", "%%"))
			i += end + 2
			continue
		}
		matched := false
		for _, el := range formatElements {
			if !strings.HasPrefix(upper[i:], el.elem) {
				continue
			}
			matched, sawElement = true, true
			switch {
			case el.elem == "FF":
				n := 9 // FF alone is FF9
				i += 2
				if i < len(f) && f[i] >= '0' && f[i] <= '9' {
					n = int(f[i] - '0')
					i++
				}
				flush()
				if n > 0 {
					pieces = append(pieces, formatPiece{fracLen: n})
				}
			case strings.HasPrefix(el.elem, "TZ"):
				flush()
				pieces = append(pieces, formatPiece{fracLen: -1, offset: el.elem})
				i += len(el.elem)
			default:
				text.WriteString(el.strftime)
				i += len(el.elem)
			}
			break
		}
		if matched {
			continue
		}
		switch f[i] {
		case '9', '0', '$', 'X', 'x':
			return nil, false // numeric format model
		case '%':
			text.WriteString("%%")
		default:
			text.WriteByte(f[i])
		}
		i++
	}
	flush()
	return pieces, sawElement
}

// formatLiteral returns the unquoted contents of a single-quoted SQL string
// literal, or false if arg is not one.
func formatLiteral(arg string) (string, bool) {
	arg = strings.TrimSpace(arg)
	if len(arg) < 2 || arg[0] != '\'' || arg[len(arg)-1] != '\'' {
		return "", false
	}
	inner := arg[1 : len(arg)-1]
	if strings.Contains(strings.ReplaceAll(inner, "''", ""), "'") {
		return "", false
	}
	return strings.ReplaceAll(inner, "''", "'"), true
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// strftimeExpr renders TO_CHAR(expr, fmt) as DuckDB SQL, or false if fmt is
// not a literal date/time format.
func strftimeExpr(expr, fmtArg string) (string, bool) {
	f, ok := formatLiteral(fmtArg)
	if !ok || strings.EqualFold(strings.TrimSpace(f), "AUTO") {
		return "", false
	}
	pieces, ok := parseDateFormat(f)
	if !ok {
		return "", false
	}
	parts := make([]string, 0, len(pieces))
	for _, p := range pieces {
		switch {
		case p.fracLen > 0:
			// %f is microseconds; pad to nanoseconds, then cut to n digits.
			parts = append(parts, "substr(strftime("+expr+", '%f') || '000', 1, "+strconv.Itoa(p.fracLen)+")")
		case p.fracLen == -1:
			// %z prints the offset as +HH or +HH:MM.
			z := "strftime(" + expr + ", '%z')"
			hhmm := "(CASE WHEN length(" + z + ") = 3 THEN " + z + " || ':00' ELSE " + z + " END)"
			switch p.offset {
			case "TZH:TZM":
				parts = append(parts, hhmm)
			case "TZHTZM":
				parts = append(parts, "replace("+hhmm+", ':', '')")
			case "TZH":
				parts = append(parts, "substr("+hhmm+", 1, 3)")
			case "TZM":
				parts = append(parts, "substr("+hhmm+", 5, 2)")
			}
		default:
			parts = append(parts, "strftime("+expr+", "+sqlString(p.strftime)+")")
		}
	}
	if len(parts) == 1 {
		return parts[0], true
	}
	return "(" + strings.Join(parts, " || ") + ")", true
}

// strptimeFormat renders a Snowflake parse format as a DuckDB strptime format
// literal, or false if it is not a literal date/time format.
func strptimeFormat(fmtArg string) (string, bool) {
	f, ok := formatLiteral(fmtArg)
	if !ok || strings.EqualFold(strings.TrimSpace(f), "AUTO") {
		return "", false
	}
	pieces, ok := parseDateFormat(f)
	if !ok {
		return "", false
	}
	var b strings.Builder
	for _, p := range pieces {
		switch {
		case p.fracLen > 0:
			b.WriteString("%f")
		case p.fracLen == -1:
			b.WriteString("%z")
		default:
			b.WriteString(p.strftime)
		}
	}
	return sqlString(b.String()), true
}
