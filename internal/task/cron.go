package task

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// nextCronTime returns the next time at or after `after` that matches the
// given 5-field cron expression. Supported fields:
//
//	minute  hour  dayOfMonth  month  dayOfWeek
//
// Each field accepts:
//   - `*`                 — any value
//   - literal integer     — e.g. `5`
//   - comma-separated list — e.g. `0,15,30`
//   - range               — e.g. `9-17`
//   - step                — e.g. `*/5` or `0-30/10`
//
// Day-of-week is 0–6 with 0 = Sunday (Snowflake convention).
//
// Cron strings may also include a trailing IANA timezone (Snowflake-style
// `USING CRON 0 9 * * 1-5 America/Los_Angeles`). If supplied, scheduling is
// evaluated in that zone; absent, UTC is used.
//
// nextCronTime stops searching after four years of clock advancement and
// returns an error — protects against impossible specs like `0 0 31 2 *`.
func nextCronTime(expr string, after time.Time) (time.Time, error) {
	fields, loc, err := parseCronExpr(expr)
	if err != nil {
		return time.Time{}, err
	}

	t := after.In(loc).Truncate(time.Minute).Add(time.Minute)
	limit := after.Add(4 * 365 * 24 * time.Hour)
	for !t.After(limit) {
		if cronMatches(fields, t) {
			return t, nil
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, fmt.Errorf("cron: no match within four years of %s", after.Format(time.RFC3339))
}

type cronFields struct {
	minute     []int
	hour       []int
	dayOfMonth []int
	month      []int
	dayOfWeek  []int
}

func parseCronExpr(expr string) (cronFields, *time.Location, error) {
	parts := strings.Fields(strings.TrimSpace(expr))
	if len(parts) < 5 {
		return cronFields{}, nil, fmt.Errorf("cron: expected at least 5 fields, got %d", len(parts))
	}

	loc := time.UTC
	if len(parts) > 5 {
		// Snowflake: the trailing field is the IANA tz name.
		zoneName := parts[5]
		z, err := time.LoadLocation(zoneName)
		if err != nil {
			return cronFields{}, nil, fmt.Errorf("cron: unknown timezone %q: %w", zoneName, err)
		}
		loc = z
	}

	specs := []struct {
		field   string
		min, mx int
	}{
		{parts[0], 0, 59}, // minute
		{parts[1], 0, 23}, // hour
		{parts[2], 1, 31}, // day of month
		{parts[3], 1, 12}, // month
		{parts[4], 0, 6},  // day of week (0=Sun)
	}
	parsed := make([][]int, len(specs))
	for i, s := range specs {
		vals, err := parseCronField(s.field, s.min, s.mx)
		if err != nil {
			return cronFields{}, nil, err
		}
		parsed[i] = vals
	}
	return cronFields{
		minute:     parsed[0],
		hour:       parsed[1],
		dayOfMonth: parsed[2],
		month:      parsed[3],
		dayOfWeek:  parsed[4],
	}, loc, nil
}

// parseCronField parses a single cron field into the list of integers it
// expands to. Returns nil for `*`, otherwise returns the explicit value list.
func parseCronField(field string, minVal, maxVal int) ([]int, error) {
	if field == "*" {
		return nil, nil
	}
	parts := strings.Split(field, ",")
	out := make([]int, 0, 8)
	seen := make(map[int]struct{})
	for _, p := range parts {
		// Step: `range/step` or `*/step`.
		step := 1
		if idx := strings.Index(p, "/"); idx >= 0 {
			stepStr := p[idx+1:]
			n, err := strconv.Atoi(stepStr)
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("cron: invalid step %q in %q", stepStr, field)
			}
			step = n
			p = p[:idx]
		}
		var start, end int
		switch {
		case p == "*":
			start, end = minVal, maxVal
		case strings.Contains(p, "-"):
			rng := strings.SplitN(p, "-", 2)
			a, errA := strconv.Atoi(rng[0])
			b, errB := strconv.Atoi(rng[1])
			if errA != nil || errB != nil {
				return nil, fmt.Errorf("cron: invalid range %q in %q", p, field)
			}
			start, end = a, b
		default:
			n, err := strconv.Atoi(p)
			if err != nil {
				return nil, fmt.Errorf("cron: invalid value %q in %q", p, field)
			}
			start, end = n, n
		}
		if start < minVal || end > maxVal || start > end {
			return nil, fmt.Errorf("cron: out-of-range %d-%d (want %d-%d) in %q", start, end, minVal, maxVal, field)
		}
		for v := start; v <= end; v += step {
			if _, dup := seen[v]; dup {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out, nil
}

// cronMatches reports whether t satisfies every set field. A nil list ==
// wildcard. Day-of-month and day-of-week are OR'd when both are constrained,
// matching Vixie-cron semantics (Snowflake follows the same rule).
func cronMatches(f cronFields, t time.Time) bool {
	if !contains(f.minute, t.Minute()) {
		return false
	}
	if !contains(f.hour, t.Hour()) {
		return false
	}
	if !contains(f.month, int(t.Month())) {
		return false
	}
	domConstrained := f.dayOfMonth != nil
	dowConstrained := f.dayOfWeek != nil
	domHit := !domConstrained || contains(f.dayOfMonth, t.Day())
	dowHit := !dowConstrained || contains(f.dayOfWeek, int(t.Weekday()))
	if domConstrained && dowConstrained {
		// Vixie-cron OR rule.
		return domHit || dowHit
	}
	return domHit && dowHit
}

func contains(vals []int, x int) bool {
	if vals == nil { // wildcard
		return true
	}
	for _, v := range vals {
		if v == x {
			return true
		}
	}
	return false
}
