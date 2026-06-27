package limiter

import (
	"sync"
	"testing"
	"time"
)

// TestImprovedBasicAllow tests basic allow functionality.
func TestImprovedBasicAllow(t *testing.T) {
	limiter := NewImprovedSlidingWindowLimiter(3, 1*time.Second, 100*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	// First 3 requests should be allowed
	for i := 0; i < 3; i++ {
		if !limiter.Allow("alice") {
			t.Errorf("Expected request %d to be allowed", i+1)
		}
	}

	// Fourth request should be rejected
	if limiter.Allow("alice") {
		t.Error("Expected fourth request to be rejected")
	}
}

// TestImprovedMultipleUsers tests independent limits for different users.
func TestImprovedMultipleUsers(t *testing.T) {
	limiter := NewImprovedSlidingWindowLimiter(2, 1*time.Second, 100*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	// Alice uses her 2 requests
	limiter.Allow("alice")
	limiter.Allow("alice")
	if limiter.Allow("alice") {
		t.Error("Expected alice's third request to be rejected")
	}

	// Bob should have independent limit
	limiter.Allow("bob")
	limiter.Allow("bob")
	if limiter.Allow("bob") {
		t.Error("Expected bob's third request to be rejected")
	}

	// Charlie should also have independent limit
	if !limiter.Allow("charlie") {
		t.Error("Expected charlie's first request to be allowed")
	}
}

// TestImprovedWindowExpiration tests that expired timestamps are removed.
func TestImprovedWindowExpiration(t *testing.T) {
	window := 100 * time.Millisecond
	limiter := NewImprovedSlidingWindowLimiter(2, window, 50*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	// Use 2 requests
	limiter.Allow("user1")
	limiter.Allow("user1")

	// Third should be rejected
	if limiter.Allow("user1") {
		t.Error("Expected third request to be rejected")
	}

	// Wait for window to expire
	time.Sleep(window + 10*time.Millisecond)

	// Now should be allowed
	if !limiter.Allow("user1") {
		t.Error("Expected request to be allowed after window expiration")
	}
}

// TestImprovedMetrics tests the metrics collection.
func TestImprovedMetrics(t *testing.T) {
	limiter := NewImprovedSlidingWindowLimiter(2, 1*time.Second, 100*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	// Make some requests
	limiter.Allow("user1") // allowed, count=1
	limiter.Allow("user1") // allowed, count=2
	limiter.Allow("user1") // blocked, count=2 >= 2
	limiter.Allow("user2") // allowed, count=1
	limiter.Allow("user2") // allowed, count=2
	limiter.Allow("user2") // blocked, count=2 >= 2

	metrics := limiter.GetMetrics()

	if metrics.AllowedRequests != 4 {
		t.Errorf("Expected 4 allowed requests, got %d", metrics.AllowedRequests)
	}
	if metrics.BlockedRequests != 2 {
		t.Errorf("Expected 2 blocked requests, got %d", metrics.BlockedRequests)
	}
	if metrics.ActiveUsers != 2 {
		t.Errorf("Expected 2 active users, got %d", metrics.ActiveUsers)
	}
}

// TestImprovedInactiveUserCleanup tests that inactive users are removed.
func TestImprovedInactiveUserCleanup(t *testing.T) {
	cleanupInterval := 100 * time.Millisecond
	inactivityTTL := 150 * time.Millisecond
	limiter := NewImprovedSlidingWindowLimiter(5, 1*time.Second, cleanupInterval, inactivityTTL)
	defer limiter.Stop()

	// Create some users
	limiter.Allow("active_user")
	limiter.Allow("inactive_user")

	metrics := limiter.GetMetrics()
	if metrics.ActiveUsers != 2 {
		t.Errorf("Expected 2 active users initially, got %d", metrics.ActiveUsers)
	}

	// Wait for inactive user to be cleaned up
	time.Sleep(inactivityTTL + cleanupInterval + 50*time.Millisecond)

	// inactive_user should be removed, but we'll make a request with active_user to ensure it's still there
	if !limiter.Allow("active_user") {
		t.Error("Expected active_user to still exist")
	}

	// Now check metrics - should have removed inactive_user
	metrics = limiter.GetMetrics()
	if metrics.ActiveUsers != 1 {
		t.Errorf("Expected 1 active user after cleanup, got %d", metrics.ActiveUsers)
	}
}

// TestImprovedGetUserRequestCount tests the request count method.
func TestImprovedGetUserRequestCount(t *testing.T) {
	window := 100 * time.Millisecond
	limiter := NewImprovedSlidingWindowLimiter(10, window, 50*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	// Make 3 requests
	limiter.Allow("user1")
	limiter.Allow("user1")
	limiter.Allow("user1")

	count := limiter.GetUserRequestCount("user1")
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Wait for expiration
	time.Sleep(window + 10*time.Millisecond)

	count = limiter.GetUserRequestCount("user1")
	if count != 0 {
		t.Errorf("Expected count 0 after expiration, got %d", count)
	}
}

// TestImprovedConcurrentAccess tests concurrent requests from multiple goroutines.
func TestImprovedConcurrentAccess(t *testing.T) {
	limiter := NewImprovedSlidingWindowLimiter(20, 1*time.Second, 100*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	successCount := 0
	failCount := 0
	mu := &sync.Mutex{}

	done := make(chan bool, 50)

	// 50 concurrent requests from one user
	for i := 0; i < 50; i++ {
		go func() {
			if limiter.Allow("concurrent_user") {
				mu.Lock()
				successCount++
				mu.Unlock()
			} else {
				mu.Lock()
				failCount++
				mu.Unlock()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 50; i++ {
		<-done
	}

	if successCount != 20 {
		t.Errorf("Expected 20 successful requests, got %d", successCount)
	}
	if failCount != 30 {
		t.Errorf("Expected 30 failed requests, got %d", failCount)
	}

	metrics := limiter.GetMetrics()
	if metrics.AllowedRequests != 20 {
		t.Errorf("Expected metrics to show 20 allowed, got %d", metrics.AllowedRequests)
	}
	if metrics.BlockedRequests != 30 {
		t.Errorf("Expected metrics to show 30 blocked, got %d", metrics.BlockedRequests)
	}
}

// TestImprovedReset tests the reset functionality.
func TestImprovedReset(t *testing.T) {
	limiter := NewImprovedSlidingWindowLimiter(2, 10*time.Second, 100*time.Millisecond, 5*time.Second)
	defer limiter.Stop()

	// Make requests - all should be allowed with limit=2
	if !limiter.Allow("user1") {
		t.Error("Expected user1 request 1 to be allowed")
	}
	if !limiter.Allow("user1") {
		t.Error("Expected user1 request 2 to be allowed")
	}
	if !limiter.Allow("user2") {
		t.Error("Expected user2 request 1 to be allowed")
	}

	metrics := limiter.GetMetrics()
	if metrics.ActiveUsers != 2 {
		t.Errorf("Expected 2 active users, got %d", metrics.ActiveUsers)
	}
	if metrics.AllowedRequests != 3 {
		t.Errorf("Expected 3 allowed requests before reset, got %d", metrics.AllowedRequests)
	}

	// Reset
	limiter.Reset()

	// Check metrics after reset
	metrics = limiter.GetMetrics()
	if metrics.ActiveUsers != 0 {
		t.Errorf("Expected 0 active users after reset, got %d", metrics.ActiveUsers)
	}

	// Should allow requests again - each user should start fresh
	if !limiter.Allow("user1") {
		t.Error("Expected user1 to be able to make request after reset")
	}
	if !limiter.Allow("user2") {
		t.Error("Expected user2 to be able to make request after reset")
	}

	metrics = limiter.GetMetrics()
	if metrics.ActiveUsers != 2 {
		t.Errorf("Expected 2 active users after new requests, got %d", metrics.ActiveUsers)
	}
}

// TestRingBuffer tests the ring buffer implementation.
func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer(3)

	now := time.Now()
	t1 := now.Add(0 * time.Second)
	t2 := now.Add(1 * time.Second)
	t3 := now.Add(2 * time.Second)
	t4 := now.Add(3 * time.Second)

	// Add 3 timestamps
	rb.Push(t1)
	rb.Push(t2)
	rb.Push(t3)

	if rb.Count() != 3 {
		t.Errorf("Expected count 3, got %d", rb.Count())
	}

	// Remove expired (anything before 500ms, so only t1 is removed)
	cutoff := now.Add(500 * time.Millisecond)
	count := rb.RemoveExpired(cutoff)

	if count != 2 {
		t.Errorf("Expected 2 remaining (t2, t3), got %d", count)
	}

	// Remove expired (anything before 1.5s, so t2 is removed)
	cutoff = now.Add(1500 * time.Millisecond)
	count = rb.RemoveExpired(cutoff)

	if count != 1 {
		t.Errorf("Expected 1 remaining (t3), got %d", count)
	}

	// Add another timestamp
	rb.Push(t4)
	if rb.Count() != 2 {
		t.Errorf("Expected count 2 after push, got %d", rb.Count())
	}
}

// BenchmarkImprovedAllow benchmarks the improved Allow method.
func BenchmarkImprovedAllow(b *testing.B) {
	limiter := NewImprovedSlidingWindowLimiter(100, 1*time.Second, 1*time.Minute, 5*time.Minute)
	defer limiter.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow("bench")
	}
}

// BenchmarkImprovedAllowMultipleUsers benchmarks with multiple users.
func BenchmarkImprovedAllowMultipleUsers(b *testing.B) {
	limiter := NewImprovedSlidingWindowLimiter(100, 1*time.Second, 1*time.Minute, 5*time.Minute)
	defer limiter.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		userID := "user" + string(rune((i%100)+'0'))
		limiter.Allow(userID)
	}
}

// BenchmarkImprovedConcurrent benchmarks concurrent access.
func BenchmarkImprovedConcurrent(b *testing.B) {
	limiter := NewImprovedSlidingWindowLimiter(100, 1*time.Second, 1*time.Minute, 5*time.Minute)
	defer limiter.Stop()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow("concurrent_bench")
		}
	})
}
