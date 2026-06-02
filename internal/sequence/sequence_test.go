package sequence

import (
	"sync"
	"testing"
)

func TestNextVal(t *testing.T) {
	m := NewManager()
	if err := m.CreateSequence("DB1", "PUBLIC", "SEQ1", 1, 1); err != nil {
		t.Fatalf("CreateSequence failed: %v", err)
	}

	// First call returns Start value.
	v, err := m.NextVal("DB1", "PUBLIC", "SEQ1")
	if err != nil {
		t.Fatalf("NextVal failed: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}

	// Second call returns Start + Increment.
	v, err = m.NextVal("DB1", "PUBLIC", "SEQ1")
	if err != nil {
		t.Fatalf("NextVal failed: %v", err)
	}
	if v != 2 {
		t.Fatalf("expected 2, got %d", v)
	}

	// Test with a larger increment.
	if err := m.CreateSequence("DB1", "PUBLIC", "SEQ2", 10, 5); err != nil {
		t.Fatalf("CreateSequence failed: %v", err)
	}
	v, _ = m.NextVal("DB1", "PUBLIC", "SEQ2")
	if v != 10 {
		t.Fatalf("expected 10, got %d", v)
	}
	v, _ = m.NextVal("DB1", "PUBLIC", "SEQ2")
	if v != 15 {
		t.Fatalf("expected 15, got %d", v)
	}
	v, _ = m.NextVal("DB1", "PUBLIC", "SEQ2")
	if v != 20 {
		t.Fatalf("expected 20, got %d", v)
	}
}

func TestConcurrentNextVal(t *testing.T) {
	m := NewManager()
	if err := m.CreateSequence("DB1", "PUBLIC", "CSEQ", 0, 1); err != nil {
		t.Fatalf("CreateSequence failed: %v", err)
	}

	const goroutines = 100
	var wg sync.WaitGroup
	results := make(chan int64, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			v, err := m.NextVal("DB1", "PUBLIC", "CSEQ")
			if err != nil {
				t.Errorf("NextVal failed: %v", err)
				return
			}
			results <- v
		}()
	}
	wg.Wait()
	close(results)

	// All values must be unique.
	seen := make(map[int64]bool)
	for v := range results {
		if seen[v] {
			t.Fatalf("duplicate value: %d", v)
		}
		seen[v] = true
	}
	if len(seen) != goroutines {
		t.Fatalf("expected %d unique values, got %d", goroutines, len(seen))
	}
}

func TestCurrentVal(t *testing.T) {
	m := NewManager()
	if err := m.CreateSequence("DB1", "PUBLIC", "SEQ_C", 5, 2); err != nil {
		t.Fatalf("CreateSequence failed: %v", err)
	}

	// CurrentVal before any NextVal returns the start value.
	v, err := m.CurrentVal("DB1", "PUBLIC", "SEQ_C")
	if err != nil {
		t.Fatalf("CurrentVal failed: %v", err)
	}
	if v != 5 {
		t.Fatalf("expected 5, got %d", v)
	}

	// Advance once.
	_, _ = m.NextVal("DB1", "PUBLIC", "SEQ_C")

	// CurrentVal should now reflect the update (Start + Increment = 7).
	v, err = m.CurrentVal("DB1", "PUBLIC", "SEQ_C")
	if err != nil {
		t.Fatalf("CurrentVal failed: %v", err)
	}
	if v != 7 {
		t.Fatalf("expected 7, got %d", v)
	}

	// Non-existent sequence.
	_, err = m.CurrentVal("DB1", "PUBLIC", "NOPE")
	if err == nil {
		t.Fatal("expected error for non-existent sequence")
	}
}
