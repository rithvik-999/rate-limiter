package limiter

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestTokenBucket_NewBucketStartsFull(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	rl := NewTokenBucketLimiterWithClock(5, 2.0, clock)
	defer rl.Stop()

	for i := 0; i < 5; i++ {
		if !rl.Allow("alice") {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
	}

	if rl.Allow("alice") {
		t.Fatal("expected request to be blocked after bucket is empty")
	}
}

func TestTokenBucket_ConsumesAndBlocks(t *testing.T) {
	clock := newFakeClock(time.Unix(200, 0))
	rl := NewTokenBucketLimiterWithClock(2, 0, clock)
	defer rl.Stop()

	if !rl.Allow("bob") {
		t.Fatal("expected first request to be allowed")
	}
	if !rl.Allow("bob") {
		t.Fatal("expected second request to be allowed")
	}
	if rl.Allow("bob") {
		t.Fatal("expected third request to be blocked")
	}

	m := rl.Metrics()
	if m.AllowedRequests != 2 || m.BlockedRequests != 1 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestTokenBucket_RefillAfterTimeAdvance(t *testing.T) {
	clock := newFakeClock(time.Unix(300, 0))
	rl := NewTokenBucketLimiterWithClock(3, 1.0, clock)
	defer rl.Stop()

	for i := 0; i < 3; i++ {
		rl.Allow("charlie")
	}
	if rl.Allow("charlie") {
		t.Fatal("expected block when no tokens remain")
	}

	clock.Advance(2 * time.Second)

	if !rl.Allow("charlie") {
		t.Fatal("expected allow after refill")
	}
	if !rl.Allow("charlie") {
		t.Fatal("expected second allow after refill")
	}
	if rl.Allow("charlie") {
		t.Fatal("expected block after consuming refilled tokens")
	}
}

func TestTokenBucket_CapacityNeverExceeded(t *testing.T) {
	clock := newFakeClock(time.Unix(400, 0))
	rl := NewTokenBucketLimiterWithClock(4, 10.0, clock)
	defer rl.Stop()

	if !rl.Allow("dana") {
		t.Fatal("expected first request to be allowed")
	}

	clock.Advance(10 * time.Second)

	for i := 0; i < 4; i++ {
		if !rl.Allow("dana") {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
	}

	if rl.Allow("dana") {
		t.Fatal("expected request to be blocked because capacity is capped")
	}
}

func TestTokenBucket_CleanupInactiveBuckets(t *testing.T) {
	clock := newFakeClock(time.Unix(500, 0))
	rl := NewTokenBucketLimiterWithOptions(3, 1.0, clock, 0, 5*time.Second)
	defer rl.Stop()

	if !rl.Allow("eve") {
		t.Fatal("expected eve request to be allowed")
	}
	if !rl.Allow("frank") {
		t.Fatal("expected frank request to be allowed")
	}

	clock.Advance(6 * time.Second)
	if !rl.Allow("eve") {
		t.Fatal("expected eve request to be allowed after time advance")
	}

	rl.CleanupInactiveBuckets()

	if rl.ActiveBuckets() != 1 {
		t.Fatalf("expected 1 active bucket after cleanup, got %d", rl.ActiveBuckets())
	}
}

func TestTokenBucket_ConcurrentRequests(t *testing.T) {
	rl := NewTokenBucketLimiter(100, 0)
	defer rl.Stop()

	const goroutines = 300
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rl.Allow("shared-user")
		}()
	}
	wg.Wait()

	m := rl.Metrics()
	if m.AllowedRequests != 100 {
		t.Fatalf("expected 100 allowed requests, got %d", m.AllowedRequests)
	}
	if m.BlockedRequests != 200 {
		t.Fatalf("expected 200 blocked requests, got %d", m.BlockedRequests)
	}
}

func TestTokenBucket_MultipleUsersIndependentBuckets(t *testing.T) {
	clock := newFakeClock(time.Unix(600, 0))
	rl := NewTokenBucketLimiterWithClock(1, 0, clock)
	defer rl.Stop()

	if !rl.Allow("user1") {
		t.Fatal("expected user1 first request to be allowed")
	}
	if rl.Allow("user1") {
		t.Fatal("expected user1 second request to be blocked")
	}
	if !rl.Allow("user2") {
		t.Fatal("expected user2 first request to be allowed")
	}
	if rl.Allow("user2") {
		t.Fatal("expected user2 second request to be blocked")
	}

	if rl.ActiveBuckets() != 2 {
		t.Fatalf("expected 2 active buckets, got %d", rl.ActiveBuckets())
	}
}
