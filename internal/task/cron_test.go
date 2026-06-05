package task

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return tt
}

func TestNextCronTime_EveryMinuteWildcard(t *testing.T) {
	t.Parallel()
	got, err := nextCronTime("* * * * *", mustTime(t, "2026-01-01T00:00:30Z"))
	if err != nil {
		t.Fatal(err)
	}
	want := mustTime(t, "2026-01-01T00:01:00Z")
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestNextCronTime_TopOfHour(t *testing.T) {
	t.Parallel()
	got, err := nextCronTime("0 * * * *", mustTime(t, "2026-01-01T00:05:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	want := mustTime(t, "2026-01-01T01:00:00Z")
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestNextCronTime_Weekday9to17EveryHalfHour(t *testing.T) {
	t.Parallel()
	// 9 AM through 4:30 PM, every 30 minutes, weekdays only.
	// 2026-01-01 is a Thursday.
	got, err := nextCronTime("0/30 9-16 * * 1-5", mustTime(t, "2026-01-01T08:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	want := mustTime(t, "2026-01-01T09:00:00Z")
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
	// Step past the last weekday slot, expect next Monday's 9 AM.
	// 2026-01-02 is Friday; 2026-01-03 Saturday; 2026-01-05 Monday.
	got, err = nextCronTime("0/30 9-16 * * 1-5", mustTime(t, "2026-01-02T16:30:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	want = mustTime(t, "2026-01-05T09:00:00Z")
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestNextCronTime_VixieDayORRule(t *testing.T) {
	t.Parallel()
	// 9 AM on the 1st OR any Sunday — Vixie OR semantics.
	// Starting Tuesday 2026-01-06, next match is Sunday 2026-01-11 at 9 AM
	// (1st of month already passed for January).
	got, err := nextCronTime("0 9 1 * 0", mustTime(t, "2026-01-06T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	want := mustTime(t, "2026-01-11T09:00:00Z")
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestNextCronTime_Timezone(t *testing.T) {
	t.Parallel()
	// 9 AM Los Angeles == 17:00 UTC in winter (PST = UTC-8).
	got, err := nextCronTime("0 9 * * * America/Los_Angeles", mustTime(t, "2026-01-15T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	want := mustTime(t, "2026-01-15T17:00:00Z")
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestNextCronTime_InvalidExpressions(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"* * * *",            // too few fields
		"60 * * * *",         // minute out of range
		"* 25 * * *",         // hour out of range
		"* * * * 0-8",        // dow out of range
		"* * * * * Atlantis", // unknown tz
		"abc * * * *",        // garbage
	}
	for _, c := range cases {
		_, err := nextCronTime(c, time.Now().UTC())
		if err == nil {
			t.Errorf("expected error for %q", c)
		}
	}
}

func TestNextCronTime_ImpossibleExpression(t *testing.T) {
	t.Parallel()
	// "On 31 Feb" — never matches. Parser accepts; nextCronTime errors after
	// the four-year search window. We just verify the error surfaces.
	_, err := nextCronTime("0 0 31 2 *", mustTime(t, "2026-01-01T00:00:00Z"))
	if err == nil {
		t.Errorf("expected no-match error for impossible cron")
	}
}
