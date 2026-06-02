package copyinto

import (
	"strconv"
	"strings"
)

// DefaultCSV returns the default file format for CSV files.
func DefaultCSV() FileFormat {
	return FileFormat{
		Type:                       "CSV",
		Compression:                "AUTO",
		FieldDelimiter:             ",",
		RecordDelimiter:            "\n",
		SkipHeader:                 0,
		DateFormat:                 "AUTO",
		TimestampFormat:            "AUTO",
		NullIf:                    []string{"\\N"},
		TrimSpace:                  false,
		ErrorOnColumnCountMismatch: true,
		StripOuterArray:            false,
	}
}

// DefaultJSON returns the default file format for JSON files.
func DefaultJSON() FileFormat {
	return FileFormat{
		Type:            "JSON",
		Compression:     "AUTO",
		FieldDelimiter:  "",
		RecordDelimiter: "\n",
		DateFormat:      "AUTO",
		TimestampFormat: "AUTO",
		StripOuterArray: false,
	}
}

// DefaultParquet returns the default file format for Parquet files.
func DefaultParquet() FileFormat {
	return FileFormat{
		Type:        "PARQUET",
		Compression: "AUTO",
	}
}

// ParseFileFormatOptions parses a map of option key-value pairs into a FileFormat.
// Keys are case-insensitive.
func ParseFileFormatOptions(options map[string]string) FileFormat {
	// Start with CSV defaults; the TYPE option will override.
	ff := DefaultCSV()

	for k, v := range options {
		key := strings.ToUpper(strings.TrimSpace(k))
		val := strings.TrimSpace(v)
		// Strip surrounding quotes if present.
		val = strings.Trim(val, "'\"")

		switch key {
		case "TYPE":
			upper := strings.ToUpper(val)
			switch upper {
			case "CSV":
				ff = DefaultCSV()
			case "JSON":
				ff = DefaultJSON()
			case "PARQUET":
				ff = DefaultParquet()
			case "AVRO":
				ff.Type = "AVRO"
			case "ORC":
				ff.Type = "ORC"
			default:
				ff.Type = upper
			}
		case "COMPRESSION":
			ff.Compression = strings.ToUpper(val)
		case "FIELD_DELIMITER":
			ff.FieldDelimiter = val
		case "RECORD_DELIMITER":
			ff.RecordDelimiter = val
		case "SKIP_HEADER":
			if n, err := strconv.Atoi(val); err == nil {
				ff.SkipHeader = n
			}
		case "DATE_FORMAT":
			ff.DateFormat = val
		case "TIMESTAMP_FORMAT":
			ff.TimestampFormat = val
		case "NULL_IF":
			ff.NullIf = parseNullIf(val)
		case "TRIM_SPACE":
			ff.TrimSpace = strings.EqualFold(val, "TRUE")
		case "ERROR_ON_COLUMN_COUNT_MISMATCH":
			ff.ErrorOnColumnCountMismatch = strings.EqualFold(val, "TRUE")
		case "STRIP_OUTER_ARRAY":
			ff.StripOuterArray = strings.EqualFold(val, "TRUE")
		}
	}

	// Re-apply the TYPE if it was set (the loop order is not guaranteed).
	if t, ok := options["TYPE"]; ok {
		ff.Type = strings.ToUpper(strings.Trim(strings.TrimSpace(t), "'\""))
	} else if t, ok := options["type"]; ok {
		ff.Type = strings.ToUpper(strings.Trim(strings.TrimSpace(t), "'\""))
	}

	return ff
}

// parseNullIf parses a NULL_IF value like ('', 'NULL') into a string slice.
func parseNullIf(val string) []string {
	val = strings.TrimSpace(val)
	val = strings.TrimPrefix(val, "(")
	val = strings.TrimSuffix(val, ")")
	parts := strings.Split(val, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'\"")
		result = append(result, p)
	}
	return result
}
