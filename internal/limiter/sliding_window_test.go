package limiter

import (
	"testing"
	"time"
)

// TestBasicAllow tests that requests are allowed up to the limit.
func TestBasicAllow(t *testing.T) {
	limiter := NewSlidingWindowLimiter(3, 1*time.Second)

	// First 3 requests should be allowed
	if !limiter.Allow("alice") {
		t.Error("Expected first request to be allowed")
	}
	if !limiter.Allow("alice") {
		t.Error("Expected second request to be allowed")
	}
	if !limiter.Allow("alice") {
		t.Error("Expected third request to be allowed")
	}

	// Fourth request should be rejected
	if limiter.Allow("alice") {
		t.Error("Expected fourth request to be rejected")
	}
}

// TestRejection tests that requests are rejected once limit is reached.
func TestRejection(t *testing.T) {
	limiter := NewSlidingWindowLimiter(2, 1*time.Second)

	// Allow first 2 requests
	limiter.Allow("bob")
	limiter.Allow("bob")

	// Next 5 requests should be rejected
	for i := 0; i < 5; i++ {
		if limiter.Allow("bob") {
			t.Errorf("Expected request %d to be rejected", i+1)
		}
	}
}

// TestMultipleUsers tests that different users have independent limits.
func TestMultipleUsers(t *testing.T) {
	limiter := NewSlidingWindowLimiter(2, 1*time.Second)

	// Alice uses her 2 requests
	if !limiter.Allow("alice") {
		t.Error("Expected alice's first request to be allowed")
	}
	if !limiter.Allow("alice") {
		t.Error("Expected alice's second request to be allowed")
	}
	if limiter.Allow("alice") {
		t.Error("Expected alice's third request to be rejected")
	}

	// Bob should still have his own limit of 2
	if !limiter.Allow("bob") {
		t.Error("Expected bob's first request to be allowed")
	}
	if !limiter.Allow("bob") {
		t.Error("Expected bob's second request to be allowed")
	}
	if limiter.Allow("bob") {
		t.Error("Expected bob's third request to be rejected")
	}
}

// TestWindowExpiration tests that old timestamps are removed after the window expires.
func TestWindowExpiration(t *testing.T) {
	window := 100 * time.Millisecond
	limiter := NewSlidingWindowLimiter(2, window)

	// Use 2 requests
	if !limiter.Allow("charlie") {
		t.Error("Expected first request to be allowed")
	}
	if !limiter.Allow("charlie") {
		t.Error("Expected second request to be allowed")
	}

	// Third request should be rejected
	if limiter.Allow("charlie") {
		t.Error("Expected third request to be rejected while window is active")
	}

	// Wait for window to expire
	time.Sleep(window + 10*time.Millisecond)

	// Now request should be allowed (old ones expired)
	if !limiter.Allow("charlie") {
		t.Error("Expected request to be allowed after window expiration")
	}
}

// TestBoundaryCondition tests the exact moment of window expiration.
func TestBoundaryCondition(t *testing.T) {
	window := 50 * time.Millisecond
	limiter := NewSlidingWindowLimiter(1, window)

	// First request at time T
	if !limiter.Allow("diana") {
		t.Error("Expected first request to be allowed")
	}

	// Immediately try second request (should be rejected)
	if limiter.Allow("diana") {
		t.Error("Expected second request to be rejected immediately")
	}

	// Wait just before window expires
	time.Sleep(40 * time.Millisecond)

	// Should still be rejected
	if limiter.Allow("diana") {
		t.Error("Expected request to be rejected before window expiration")
	}

	// Wait for window to fully expire
	time.Sleep(20 * time.Millisecond)

	// Now should be allowed
	if !limiter.Allow("diana") {
		t.Error("Expected request to be allowed after window expiration")
	}
}

// TestReset tests that Reset clears all user data.
func TestReset(t *testing.T) {
	limiter := NewSlidingWindowLimiter(1, 10*time.Second)

	// Use requests from multiple users
	limiter.Allow("eve")
	limiter.Allow("frank")
	limiter.Allow("grace")

	// All should be at limit
	if limiter.Allow("eve") {
		t.Error("Expected eve's second request to be rejected")
	}
	if limiter.Allow("frank") {
		t.Error("Expected frank's second request to be rejected")
	}
	if limiter.Allow("grace") {
		t.Error("Expected grace's second request to be rejected")
	}

	// Reset
	limiter.Reset()

	// All users should be able to make requests again
	if !limiter.Allow("eve") {
		t.Error("Expected eve to make a request after reset")
	}
	if !limiter.Allow("frank") {
		t.Error("Expected frank to make a request after reset")
	}
	if !limiter.Allow("grace") {
		t.Error("Expected grace to make a request after reset")
	}
}

// TestGetUserRequestCount tests the GetUserRequestCount method.
func TestGetUserRequestCount(t *testing.T) {
	window := 100 * time.Millisecond
	limiter := NewSlidingWindowLimiter(5, window)

	// Initially count should be 0
	if limiter.GetUserRequestCount("henry") != 0 {
		t.Error("Expected initial count to be 0")
	}

	// Make 3 requests
	limiter.Allow("henry")
	limiter.Allow("henry")
	limiter.Allow("henry")

	// Count should be 3
	if limiter.GetUserRequestCount("henry") != 3 {
		t.Error("Expected count to be 3, got", limiter.GetUserRequestCount("henry"))
	}

	// Wait for window to expire
	time.Sleep(window + 10*time.Millisecond)

	// Count should be 0 after expiration
	if limiter.GetUserRequestCount("henry") != 0 {
		t.Error("Expected count to be 0 after window expiration")
	}
}

// TestConcurrentRequests tests concurrent requests from multiple goroutines.
func TestConcurrentRequests(t *testing.T) {
	limiter := NewSlidingWindowLimiter(10, 1*time.Second)
	userID := "concurrent"
	successCount := 0
	failCount := 0

	// Channel to synchronize goroutines
	done := make(chan bool, 20)

	// Launch 20 concurrent requests
	for i := 0; i < 20; i++ {
		go func() {
			if limiter.Allow(userID) {
				successCount++
			} else {
				failCount++
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Should allow exactly 10, reject 10
	if successCount != 10 {
		t.Errorf("Expected 10 successful requests, got %d", successCount)
	}
	if failCount != 10 {
		t.Errorf("Expected 10 failed requests, got %d", failCount)
	}
}

// TestPartialWindowExpiration tests that only expired timestamps are removed.
func TestPartialWindowExpiration(t *testing.T) {
	window := 100 * time.Millisecond
	limiter := NewSlidingWindowLimiter(5, window)

	// Make first 2 requests
	limiter.Allow("iris")
	limiter.Allow("iris")

	// Wait 60ms
	time.Sleep(60 * time.Millisecond)

	// Make 2 more requests
	limiter.Allow("iris")
	limiter.Allow("iris")

	// Count should be 4 (none expired yet)
	if limiter.GetUserRequestCount("iris") != 4 {
		t.Error("Expected count to be 4")
	}

	// Wait 50ms more (total 110ms, so first 2 should expire)
	time.Sleep(50 * time.Millisecond)

	// Count should be 2 (only the 2 recent ones)
	if limiter.GetUserRequestCount("iris") != 2 {
		t.Error("Expected count to be 2 after partial expiration")
	}
}

// TestEdgeCaseZeroWindow tests with a very small window.
func TestEdgeCaseZeroWindow(t *testing.T) {
	limiter := NewSlidingWindowLimiter(1, 50*time.Millisecond)

	// First request should be allowed
	if !limiter.Allow("jack") {
		t.Error("Expected first request to be allowed")
	}

	// Second request should be rejected (still within window)
	if limiter.Allow("jack") {
		t.Error("Expected second request to be rejected")
	}

	// Wait for window to expire
	time.Sleep(60 * time.Millisecond)

	// Now request should be allowed (window expired)
	if !limiter.Allow("jack") {
		t.Error("Expected request to be allowed after window expiration")
	}
}

// TestLargeLimit tests with a large request limit.
func TestLargeLimit(t *testing.T) {
	limiter := NewSlidingWindowLimiter(1000, 1*time.Second)

	// Make 1000 requests
	for i := 0; i < 1000; i++ {
		if !limiter.Allow("kate") {
			t.Errorf("Expected request %d to be allowed", i+1)
		}
	}

	// 1001st request should be rejected
	if limiter.Allow("kate") {
		t.Error("Expected 1001st request to be rejected")
	}
}

// BenchmarkAllow benchmarks the Allow method.
func BenchmarkAllow(b *testing.B) {
	limiter := NewSlidingWindowLimiter(100, 1*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow("bench")
	}
}

// BenchmarkAllowMultipleUsers benchmarks Allow with multiple different users.
func BenchmarkAllowMultipleUsers(b *testing.B) {
	limiter := NewSlidingWindowLimiter(100, 1*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		userID := "user" + string(rune((i%10)+'0'))
		limiter.Allow(userID)
	}
}
