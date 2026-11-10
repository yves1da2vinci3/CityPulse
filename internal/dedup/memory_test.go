package dedup

import "testing"

func TestSeenOrAdd(t *testing.T) {
	m := NewMemory()
	if m.SeenOrAdd("a") {
		t.Fatal("first must be false")
	}
	if !m.SeenOrAdd("a") {
		t.Fatal("second must be true")
	}
	if m.SeenOrAdd("b") {
		t.Fatal("other id first must be false")
	}
}
