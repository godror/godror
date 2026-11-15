package sieve

import (
	"sync"
	"testing"
)

func TestBasicGetPut(t *testing.T) {
	c := New[string, int](2)
	if v, ok := c.Get("missing"); ok || v != 0 {
		t.Fatalf("missing key: got %d, %v", v, ok)
	}
	c.Put("a", 1)
	c.Put("b", 2)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %d, %v", v, ok)
	}
	if n := c.Len(); n != 2 {
		t.Fatalf("Len() = %d, want 2", n)
	}
}

func TestSieveRetainsVisitedOldest(t *testing.T) {
	c := New[string, int](3)
	c.Put("a", 1) // oldest
	c.Put("b", 2)
	c.Put("c", 3)
	c.Get("a") // protect oldest without moving it
	c.Put("d", 4)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("visited oldest entry a should have survived")
	}
	if _, ok := c.Get("d"); !ok {
		t.Fatal("new entry d should be cached")
	}
}

func TestHandContinuesFromPreviousEviction(t *testing.T) {
	c := New[string, int](3)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("c", 3)
	c.Get("a")
	c.Put("d", 4) // a visited -> b evicted; hand points at c
	c.Put("e", 5) // c evicted, not a or d
	if _, ok := c.Get("c"); ok {
		t.Fatal("c should have been evicted by advancing hand")
	}
	for _, k := range []string{"a", "d", "e"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("expected %s in cache", k)
		}
	}
}

func TestAllVisitedAndWrap(t *testing.T) {
	c := New[int, string](2)
	c.Put(1, "a")
	c.Put(2, "b")
	c.Get(1)
	c.Get(2)
	c.Put(3, "c") // clears both bits, then wraps and evicts 1
	if _, ok := c.Get(1); ok {
		t.Fatal("1 should be evicted after the hand wraps")
	}
	if _, ok := c.Get(2); !ok {
		t.Fatal("2 should remain")
	}
	if _, ok := c.Get(3); !ok {
		t.Fatal("3 should remain")
	}
}

func TestUpdateAndCapacityOne(t *testing.T) {
	c := New[string, string](2)
	c.Put("a", "old")
	c.Put("b", "b")
	c.Put("a", "new") // treat update as a visit; no duplicate or reordering
	if c.Len() != 2 {
		t.Fatal("update increased cache length")
	}
	c.Put("c", "c") // b is the oldest unvisited entry
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if v, ok := c.Get("a"); !ok || v != "new" {
		t.Fatalf("Get(a) = %q, %v", v, ok)
	}

	one := New[int, int](1)
	one.Put(1, 10)
	one.Get(1)
	one.Put(2, 20)
	if one.Len() != 1 {
		t.Fatal("capacity one exceeded")
	}
	if _, ok := one.Get(1); ok {
		t.Fatal("old entry not evicted")
	}
}

func TestGenericValueAndNil(t *testing.T) {
	type key struct{ x, y int }
	type val struct{ items []string }
	c := New[key, *val](2)
	k := key{1, 2}
	c.Put(k, &val{items: []string{"x"}})
	v, ok := c.Get(k)
	if !ok || len(v.items) != 1 || v.items[0] != "x" {
		t.Fatal("generic key/value mismatch")
	}
	c.Put(key{3, 4}, nil)
	if v, ok := c.Get(key{3, 4}); !ok || v != nil {
		t.Fatal("stored nil must differ from missing key")
	}
}

func TestBadCapacityPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	New[int, int](0)
}

func TestConcurrentUse(t *testing.T) {
	c := New[int, int](16)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k := (worker + i) % 64
				c.Put(k, i)
				c.Get((k + 1) % 64)
			}
		}(worker)
	}
	wg.Wait()
	if n := c.Len(); n != 16 {
		t.Fatalf("Len() = %d, want 16", n)
	}
}
