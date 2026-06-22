package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestAllowWithinLimit(t *testing.T){
	rl := NewFixedWindowLimiter(5, time.Minute)

	for i := 0; i < 5; i++ {
		if !rl.Allow("user1"){
			t.Fatal("expected allow")
		}
	}
}

func TestBlockAfterLimit(t *testing.T){
	rl := NewFixedWindowLimiter(5, time.Minute)

	for i := 0; i < 5; i++ {
		rl.Allow("user1")
	}

	if rl.Allow("user1"){
		t.Fatal("expected block")
	}
}

func TestConcurrencyRequests(t *testing.T){
	rl := NewFixedWindowLimiter(5, time.Minute)

	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			rl.Allow("user1")
		}()
	}

	wg.Wait()
}